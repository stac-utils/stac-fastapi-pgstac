"""Catalogs client implementation for pgstac."""

import json
import logging
from typing import Any, cast

import attr
from buildpg import render
from fastapi import HTTPException
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.errors import NotFoundError
from stac_fastapi.types.stac import ItemCollection
from stac_fastapi_catalogs_extension.client import AsyncBaseCatalogsClient
from stac_fastapi_catalogs_extension.types import Children
from starlette.requests import Request
from starlette.responses import JSONResponse

from stac_fastapi.pgstac.extensions.catalogs.catalogs_database_logic import (
    _parse_pagination_token,
)
from stac_fastapi.pgstac.extensions.catalogs.catalogs_links import (
    CatalogLinks,
    ChildLinks,
    SubCatalogLinks,
)
from stac_fastapi.pgstac.models.links import (
    CollectionSearchPagingLinks,
    ItemCollectionLinks,
    filter_links,
)


def _remove_null_titles(obj: Any) -> Any:
    """Recursively remove title fields that are None from dicts and lists."""
    if isinstance(obj, dict):
        return {
            k: _remove_null_titles(v)
            for k, v in obj.items()
            if not (k == "title" and v is None)
        }
    elif isinstance(obj, list):
        return [_remove_null_titles(item) for item in obj]
    else:
        return obj


logger = logging.getLogger(__name__)


@attr.s
class CatalogsClient(AsyncBaseCatalogsClient):
    """Catalogs client implementation for pgstac.

    This client implements the AsyncBaseCatalogsClient interface and delegates
    to the database layer for all catalog operations.
    """

    database: Any = attr.ib()

    async def get_catalogs(
        self,
        limit: int | None = None,
        token: str | None = None,
        request: Request | None = None,
        **kwargs,
    ) -> JSONResponse:
        """Get all catalogs with pagination.

        Args:
            limit: The maximum number of catalogs to return.
            token: The pagination token.
            request: The FastAPI request object.
            **kwargs: Additional keyword arguments.

        Returns:
            Catalogs object containing catalogs list, total count, and pagination info.
        """
        # Check if offset is in query params (from pagination link)
        if request and not token:
            offset_param = request.query_params.get("offset")
            if offset_param:
                token = offset_param

        limit = limit or 10
        catalogs_list, total_hits, _ = await self.database.get_all_catalogs(
            token=token,
            limit=limit,
            request=request,
        )

        # Generate links dynamically for each catalog
        if request and catalogs_list:
            for catalog in catalogs_list:
                catalog_id = cast(str, catalog.get("id"))
                parent_ids_raw = catalog.get("parent_ids", [])
                parent_ids: list[str] = (
                    cast(list[str], parent_ids_raw)
                    if isinstance(parent_ids_raw, list)
                    else ([cast(str, parent_ids_raw)] if parent_ids_raw else [])
                )

                # Get child catalogs for link generation
                child_catalogs, _, _ = await self.database.get_sub_catalogs(
                    catalog_id=catalog_id,
                    limit=1000,
                    request=request,
                )
                child_catalog_ids: list[str] = (
                    [cast(str, c.get("id")) for c in child_catalogs]
                    if child_catalogs
                    else []
                )

                # Generate links
                catalog["links"] = await CatalogLinks(
                    catalog_id=catalog_id,
                    request=request,
                    parent_ids=parent_ids,
                    child_catalog_ids=child_catalog_ids,
                ).get_links(extra_links=catalog.get("links"))

                # Remove internal metadata before returning
                catalog.pop("parent_ids", None)

        pagination_links: list[dict] = []
        if request:
            offset: int = _parse_pagination_token(token)

            # Check if there are more results
            next_token_to_use = None
            if total_hits and offset + len(catalogs_list) < total_hits:
                # There are more results, generate next link
                next_offset = offset + len(catalogs_list)
                next_token_to_use = {
                    "rel": "next",
                    "type": "application/json",
                    "body": {"offset": next_offset},
                }

            pagination_links = await CollectionSearchPagingLinks(
                request=request, next=next_token_to_use, prev=None
            ).get_links()

        result_dict = {
            "catalogs": catalogs_list or [],
            "links": pagination_links,
            "numberMatched": total_hits,
            "numberReturned": len(catalogs_list) if catalogs_list else 0,
        }
        return JSONResponse(_remove_null_titles(result_dict))

    async def get_catalog(
        self, catalog_id: str, request: Request | None = None, **kwargs
    ) -> JSONResponse:
        """Get a specific catalog by ID.

        Args:
            catalog_id: The ID of the catalog to retrieve.
            request: The FastAPI request object.
            **kwargs: Additional keyword arguments.

        Returns:
            JSONResponse containing the catalog with generated links.

        Raises:
            NotFoundError: If the catalog is not found.
        """
        try:
            catalog = await self.database.find_catalog(catalog_id, request=request)

            if request:
                parent_ids_raw = catalog.get("parent_ids", [])
                parent_ids: list[str] = (
                    cast(list[str], parent_ids_raw)
                    if isinstance(parent_ids_raw, list)
                    else ([cast(str, parent_ids_raw)] if parent_ids_raw else [])
                )

                # Get child catalogs (catalogs that have this catalog in their parent_ids)
                child_catalogs, _, _ = await self.database.get_sub_catalogs(
                    catalog_id=catalog_id,
                    limit=1000,  # Get all children for link generation
                    request=request,
                )
                child_catalog_ids: list[str] = (
                    [cast(str, c.get("id")) for c in child_catalogs]
                    if child_catalogs
                    else []
                )

                catalog["links"] = await CatalogLinks(
                    catalog_id=catalog_id,
                    request=request,
                    parent_ids=parent_ids,
                    child_catalog_ids=child_catalog_ids,
                ).get_links(extra_links=catalog.get("links"))

            # Remove internal metadata before returning
            catalog.pop("parent_ids", None)

            return JSONResponse(content=catalog)
        except NotFoundError:
            raise

    async def create_catalog(
        self, catalog: dict, request: Request | None = None, **kwargs
    ) -> JSONResponse:
        """Create a new catalog.

        Args:
            catalog: The catalog dictionary or Pydantic model.
            request: The FastAPI request object.
            **kwargs: Additional keyword arguments.

        Returns:
            JSONResponse containing the created catalog with dynamically generated links.

        Raises:
            HTTPException: 409 Conflict if catalog already exists.
        """
        # Convert Pydantic model to dict if needed
        catalog_dict = cast(
            stac_types.Catalog,
            catalog.model_dump(mode="json")
            if hasattr(catalog, "model_dump")
            else catalog,
        )

        # Filter out inferred links before storing to avoid overwriting generated links
        if "links" in catalog_dict:
            catalog_dict["links"] = filter_links(catalog_dict["links"])

        # Check if catalog already exists
        catalog_id = catalog_dict.get("id")
        if catalog_id and request:
            try:
                existing = await self.database.find_catalog(catalog_id, request=request)
                if existing:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Catalog {catalog_id} already exists",
                    )
            except NotFoundError:
                # Catalog doesn't exist, proceed with creation
                pass

        success = await self.database.create_catalog(
            dict(catalog_dict), refresh=True, request=request
        )

        if not success:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create catalog {catalog_id}",
            )

        # Generate links dynamically for response
        if request:
            catalog_id = cast(str, catalog_dict.get("id"))
            parent_ids_raw = catalog_dict.get("parent_ids", [])
            parent_ids: list[str] = (
                cast(list[str], parent_ids_raw)
                if isinstance(parent_ids_raw, list)
                else ([cast(str, parent_ids_raw)] if parent_ids_raw else [])
            )

            # Get child catalogs for link generation
            child_catalogs, _, _ = await self.database.get_sub_catalogs(
                catalog_id=catalog_id,
                limit=1000,
                request=request,
            )
            child_catalog_ids: list[str] = (
                [cast(str, c.get("id")) for c in child_catalogs] if child_catalogs else []
            )

            # Generate links
            catalog_dict["links"] = await CatalogLinks(
                catalog_id=catalog_id,
                request=request,
                parent_ids=parent_ids,
                child_catalog_ids=child_catalog_ids,
            ).get_links(extra_links=catalog_dict.get("links"))

        # Remove internal metadata before returning
        catalog_dict.pop("parent_ids", None)  # type: ignore

        return JSONResponse(content=catalog_dict, status_code=201)

    async def update_catalog(
        self, catalog_id: str, catalog: dict, request: Request | None = None, **kwargs
    ) -> stac_types.Catalog:
        """Update an existing catalog.

        Args:
            catalog_id: The ID of the catalog to update.
            catalog: The updated catalog dictionary or Pydantic model.
            request: The FastAPI request object.
            **kwargs: Additional keyword arguments.

        Returns:
            The updated catalog.
        """
        # Convert Pydantic model to dict if needed
        catalog_dict = cast(
            stac_types.Catalog,
            catalog.model_dump(mode="json")
            if hasattr(catalog, "model_dump")
            else catalog,
        )

        await self.database.update_catalog(
            catalog_id, dict(catalog_dict), refresh=True, request=request
        )
        return catalog_dict

    async def delete_catalog(
        self, catalog_id: str, request: Request | None = None, **kwargs
    ) -> None:
        """Delete a catalog.

        Args:
            catalog_id: The ID of the catalog to delete.
            request: The FastAPI request object.
            **kwargs: Additional keyword arguments.
        """
        await self.database.delete_catalog(catalog_id, refresh=True, request=request)

    def _rewrite_collection_links(
        self,
        collection: dict,
        catalog_id: str,
        request: Request,
    ) -> None:
        """Rewrite collection links for scoped context."""
        collection_id = collection.get("id")
        parent_ids = collection.get("parent_ids", [])

        # For scoped endpoint, generate links pointing to this specific catalog
        collection["links"] = [
            {
                "rel": "self",
                "type": "application/json",
                "href": str(request.url),
            },
            {
                "rel": "parent",
                "type": "application/json",
                "href": str(request.base_url).rstrip("/") + f"/catalogs/{catalog_id}",
                "title": catalog_id,
            },
            {
                "rel": "root",
                "type": "application/json",
                "href": str(request.base_url).rstrip("/"),
            },
        ]

        # Add custom links from storage (non-inferred)
        if collection.get("links"):
            custom_links = filter_links(collection.get("links", []))
            collection["links"].extend(custom_links)

        # Add related links for alternative parents (poly-hierarchy)
        if parent_ids and len(parent_ids) > 1:
            for parent_id in parent_ids:
                if parent_id != catalog_id:  # Don't link to self
                    related_href = (
                        str(request.base_url).rstrip("/")
                        + f"/catalogs/{parent_id}/collections/{collection_id}"
                    )
                    if not any(
                        link.get("href") == related_href
                        for link in collection["links"]
                        if link.get("rel") == "related"
                    ):
                        collection["links"].append(
                            {
                                "rel": "related",
                                "type": "application/json",
                                "href": related_href,
                                "title": f"Collection in {parent_id}",
                            }
                        )

        # Remove internal metadata
        collection.pop("parent_ids", None)

    def _extract_limit_and_token(
        self, limit: int | None, token: str | None, request: Request | None
    ) -> tuple[int, str | None]:
        """Extract limit and token from parameters and request query params."""
        if request and not token:
            offset = request.query_params.get("offset")
            if offset:
                token = offset

        if request and not limit:
            limit_param = request.query_params.get("limit")
            if limit_param:
                try:
                    limit = int(limit_param)
                except (ValueError, TypeError):
                    limit = None

        return limit or 10, token

    async def _build_response_links(
        self,
        catalog_id: str,
        offset: int,
        original_count: int,
        total_hits: int | None,
        request: Request | None,
    ) -> list[dict]:
        """Build response-level pagination and parent links."""
        next_token_to_use = None
        if total_hits and offset + original_count < total_hits:
            next_offset = offset + original_count
            next_token_to_use = {
                "rel": "next",
                "type": "application/json",
                "body": {"offset": next_offset},
            }

        if request is None:
            response_links = []
        else:
            response_links = await CollectionSearchPagingLinks(
                request=request, next=next_token_to_use, prev=None
            ).get_links()

        # Remove title field from response links
        response_links = [
            {k: v for k, v in link.items() if k != "title"} for link in response_links
        ]

        # Add parent link if not present
        if request and not any(link.get("rel") == "parent" for link in response_links):
            response_links.append(
                {
                    "rel": "parent",
                    "type": "application/json",
                    "href": str(request.base_url).rstrip("/") + f"/catalogs/{catalog_id}",
                }
            )

        return response_links

    async def get_catalog_collections(
        self,
        catalog_id: str,
        limit: int | None = None,
        token: str | None = None,
        request: Request | None = None,
        **kwargs,
    ) -> JSONResponse:
        """Get collections linked to a catalog.

        Args:
            catalog_id: The ID of the catalog.
            limit: The maximum number of collections to return.
            token: The pagination token.
            request: The FastAPI request object.
            **kwargs: Additional keyword arguments.

        Returns:
            Collections object containing collections list, total count, and pagination info.
        """
        limit, token = self._extract_limit_and_token(limit, token, request)

        (
            collections_list,
            total_hits,
            _,
        ) = await self.database.get_catalog_collections(
            catalog_id=catalog_id,
            limit=limit,
            token=token,
            request=request,
        )

        offset: int = _parse_pagination_token(token)
        original_count = len(collections_list) if collections_list else 0

        if collections_list and len(collections_list) > limit:
            collections_list = collections_list[:limit]

        if request and collections_list:
            for collection in collections_list:
                self._rewrite_collection_links(collection, catalog_id, request)

        response_links = await self._build_response_links(
            catalog_id, offset, original_count, total_hits, request
        )

        result_dict = {
            "collections": collections_list or [],
            "links": response_links,
            "numberMatched": total_hits,
            "numberReturned": len(collections_list) if collections_list else 0,
        }
        return JSONResponse(_remove_null_titles(result_dict))

    async def _generate_sub_catalog_links(
        self, catalogs_list: list[dict], catalog_id: str, request: Request | None
    ) -> None:
        """Generate links for each sub-catalog in the list."""
        if not (request and catalogs_list):
            return

        for catalog in catalogs_list:
            sub_catalog_id = cast(str, catalog.get("id"))
            parent_ids = catalog.get("parent_ids", [])

            catalog["links"] = await SubCatalogLinks(
                catalog_id=catalog_id,
                sub_catalog_id=sub_catalog_id,
                request=request,
                parent_ids=parent_ids,
            ).get_links(extra_links=catalog.get("links"))

            catalog.pop("parent_ids", None)

    async def get_sub_catalogs(
        self,
        catalog_id: str,
        limit: int | None = None,
        token: str | None = None,
        request: Request | None = None,
        **kwargs,
    ) -> JSONResponse:
        """Get all sub-catalogs of a specific catalog with pagination.

        Args:
            catalog_id: The ID of the parent catalog.
            limit: The maximum number of sub-catalogs to return.
            token: The pagination token.
            request: The FastAPI request object.
            **kwargs: Additional keyword arguments.

        Returns:
            Catalogs object containing sub-catalogs list, total count, and pagination info.

        Raises:
            NotFoundError: If the parent catalog is not found.
        """
        try:
            catalog = await self.database.find_catalog(catalog_id, request=request)
            if not catalog:
                raise NotFoundError(f"Catalog {catalog_id} not found")
        except NotFoundError:
            raise
        except Exception as e:
            raise NotFoundError(f"Catalog {catalog_id} not found") from e

        limit, token = self._extract_limit_and_token(limit, token, request)

        catalogs_list, total_hits, _ = await self.database.get_sub_catalogs(
            catalog_id=catalog_id,
            limit=limit,
            token=token,
            request=request,
        )

        offset: int = _parse_pagination_token(token)
        original_count = len(catalogs_list) if catalogs_list else 0

        if catalogs_list and len(catalogs_list) > limit:
            catalogs_list = catalogs_list[:limit]

        await self._generate_sub_catalog_links(catalogs_list, catalog_id, request)

        pagination_links = await self._build_response_links(
            catalog_id, offset, original_count, total_hits, request
        )

        result_dict = {
            "catalogs": catalogs_list or [],
            "links": pagination_links,
            "numberMatched": total_hits,
            "numberReturned": len(catalogs_list) if catalogs_list else 0,
        }
        return JSONResponse(_remove_null_titles(result_dict))

    async def create_sub_catalog(
        self, catalog_id: str, catalog: dict, request: Request | None = None, **kwargs
    ) -> JSONResponse:
        """Create a new catalog or link an existing catalog as a sub-catalog.

        Maintains a list of parent IDs in the catalog's parent_ids field.
        Supports two modes:
        - Mode A (Creation): Full Catalog JSON body with id that doesn't exist → creates new catalog
        - Mode B (Linking): Minimal body with just id of existing catalog → links to parent

        Args:
            catalog_id: The ID of the parent catalog.
            catalog: Create or link (full Catalog or ObjectUri with id).
            request: The FastAPI request object.
            **kwargs: Additional keyword arguments.

        Returns:
            JSONResponse containing the created or linked catalog.

        Raises:
            HTTPException: 400 Bad Request if linking would create a cycle.
        """
        # Convert Pydantic model to dict if needed
        if hasattr(catalog, "model_dump"):
            catalog_dict = catalog.model_dump(mode="json")
        else:
            catalog_dict = dict(catalog) if not isinstance(catalog, dict) else catalog

        cat_id = catalog_dict.get("id")

        try:
            # Try to find existing catalog
            existing = await self.database.find_catalog(cat_id, request=request)

            # Check for cycles before linking
            if await self.database._check_cycle(cat_id, catalog_id, request=request):
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot link catalog {cat_id} as child of {catalog_id}: would create a cycle",
                )

            # Link existing catalog - add parent_id if not already present
            parent_ids = existing.get("parent_ids", [])
            if not isinstance(parent_ids, list):
                parent_ids = [parent_ids]
            if catalog_id not in parent_ids:
                parent_ids.append(catalog_id)
            existing["parent_ids"] = parent_ids
            success = await self.database.create_catalog(
                existing, refresh=True, request=request
            )
            if not success:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to link catalog {cat_id} to parent {catalog_id}",
                )
            return JSONResponse(content=existing, status_code=201)
        except HTTPException:
            # Re-raise HTTP exceptions (like cycle detection errors)
            raise
        except NotFoundError:
            # Create new catalog
            catalog_dict["type"] = "Catalog"
            catalog_dict["parent_ids"] = [catalog_id]
            success = await self.database.create_catalog(
                catalog_dict, refresh=True, request=request
            )
            if not success:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to create catalog {cat_id}",
                ) from None
            return JSONResponse(content=catalog_dict, status_code=201)

    async def create_catalog_collection(
        self, catalog_id: str, collection: dict, request: Request | None = None, **kwargs
    ) -> JSONResponse:
        """Create a collection in a catalog.

        Creates a new collection or links an existing collection to a catalog.
        Maintains a list of parent IDs in the collection's parent_ids field (poly-hierarchy).

        Supports two modes:
        - Mode A (Creation): Full Collection JSON body with id that doesn't exist → creates new collection
        - Mode B (Linking): Minimal body with just id of existing collection → links to catalog

        Args:
            catalog_id: The ID of the catalog to link the collection to.
            collection: Create or link (full Collection or ObjectUri with id).
            request: The FastAPI request object.
            **kwargs: Additional keyword arguments.

        Returns:
            JSONResponse containing the created or linked collection.
        """
        # Convert Pydantic model to dict if needed
        if hasattr(collection, "model_dump"):
            collection_dict = collection.model_dump(mode="json")
        else:
            collection_dict = (
                dict(collection) if not isinstance(collection, dict) else collection
            )

        coll_id = collection_dict.get("id")

        # Filter out inferred links before storing to avoid overwriting generated links
        if "links" in collection_dict:
            collection_dict["links"] = filter_links(collection_dict["links"])

        try:
            # Try to find existing collection
            existing = await self.database.find_collection(coll_id, request=request)
            # Link existing collection - add parent_id if not already present (poly-hierarchy)
            parent_ids = existing.get("parent_ids", [])
            if not isinstance(parent_ids, list):
                parent_ids = [parent_ids]
            if catalog_id not in parent_ids:
                parent_ids.append(catalog_id)
            existing["parent_ids"] = parent_ids
            await self.database.update_collection(
                coll_id, existing, refresh=True, request=request
            )
            return JSONResponse(content=existing, status_code=200)
        except HTTPException:
            # Re-raise HTTP exceptions
            raise
        except NotFoundError:
            # Create new collection
            collection_dict["type"] = "Collection"
            collection_dict["parent_ids"] = [catalog_id]
            success = await self.database.create_collection(
                collection_dict, refresh=True, request=request
            )
            if not success:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to create collection {coll_id}",
                ) from None
            return JSONResponse(content=collection_dict, status_code=201)

    async def get_catalog_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request | None = None,
        **kwargs,
    ) -> JSONResponse:
        """Get a collection from a catalog.

        Args:
            catalog_id: The ID of the catalog.
            collection_id: The ID of the collection.
            request: The FastAPI request object.
            **kwargs: Additional keyword arguments.

        Returns:
            JSONResponse containing the collection.
        """
        collection = await self.database.get_catalog_collection(
            catalog_id=catalog_id,
            collection_id=collection_id,
            request=request,
        )
        # Run the rewrite logic to generate links AND pop parent_ids
        if request:
            self._rewrite_collection_links(collection, catalog_id, request)
        return JSONResponse(content=collection)

    async def unlink_catalog_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request | None = None,
        **kwargs,
    ) -> None:
        """Unlink a collection from a catalog.

        Args:
            catalog_id: The ID of the catalog.
            collection_id: The ID of the collection to unlink.
            request: The FastAPI request object.
            **kwargs: Additional keyword arguments.
        """
        await self.database.unlink_collection(
            catalog_id=catalog_id,
            collection_id=collection_id,
            request=request,
        )

    def _extract_pagination_tokens(
        self, links: list[dict]
    ) -> tuple[str | None, str | None]:
        """Extract next and prev tokens from search links."""
        next_token = None
        prev_token = None
        for link in links:
            if link.get("rel") == "next" and "token=" in link.get("href", ""):
                href = link.get("href", "")
                if "token=" in href:
                    next_token = href.split("token=")[1].split("&")[0]
            elif link.get("rel") == "prev" and "token=" in link.get("href", ""):
                href = link.get("href", "")
                if "token=" in href:
                    prev_token = href.split("token=")[1].split("&")[0]
        return next_token, prev_token

    async def get_catalog_collection_items(
        self,
        catalog_id: str,
        collection_id: str,
        limit: int | None = None,
        token: str | None = None,
        request: Request | None = None,
        **kwargs,
    ) -> ItemCollection:
        """Get items from a collection in a catalog.

        Follows the same pattern as core.py's item_collection method.

        Args:
            catalog_id: The ID of the catalog.
            collection_id: The ID of the collection.
            limit: The maximum number of items to return.
            token: The pagination token.
            request: The FastAPI request object.
            **kwargs: Additional keyword arguments.

        Returns:
            ItemCollection with items and pagination links.
        """
        if request is None:
            return {
                "type": "FeatureCollection",
                "features": [],
                "links": [],
                "numberMatched": 0,
                "numberReturned": 0,
            }

        # Check if limit is in query params
        if request and not limit:
            limit_param = request.query_params.get("limit")
            if limit_param:
                try:
                    limit = int(limit_param)
                except (ValueError, TypeError):
                    limit = None

        limit = limit or 10

        # Build search request to get items from collection
        search_query = {
            "collections": [collection_id],
            "limit": limit,
        }

        if token:
            search_query["token"] = token

        # Execute search and get full item collection with links
        async with request.app.state.get_connection(request, "r") as conn:
            q, p = render(
                """
                SELECT * FROM search(:search::text::jsonb);
                """,
                search=json.dumps(search_query),
            )
            item_collection = await conn.fetchval(q, *p) or {
                "type": "FeatureCollection",
                "features": [],
                "links": [],
            }

        # Extract pagination tokens from search links
        extra_links = item_collection.get("links", [])
        next_token, prev_token = self._extract_pagination_tokens(extra_links)

        # Generate pagination links for the scoped endpoint
        from stac_fastapi.pgstac.models.links import PagingLinks

        pagination_links = await PagingLinks(
            request=request,
            next=next_token,
            prev=prev_token,
        ).get_links()

        # Generate other links using ItemCollectionLinks
        links = await ItemCollectionLinks(
            collection_id=collection_id, request=request
        ).get_links(extra_links=[])

        # Rewrite self link to point to scoped endpoint
        for link in links:
            if link.get("rel") == "self":
                link["href"] = (
                    f"{str(request.base_url).rstrip('/')}/catalogs/{catalog_id}/collections/{collection_id}/items"
                )
                if limit != 10:  # Only add limit if it's not the default
                    link["href"] += f"?limit={limit}"

        # Combine pagination links with other links
        links.extend(pagination_links)

        item_collection["links"] = links

        return cast(ItemCollection, item_collection)

    async def get_catalog_collection_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        request: Request | None = None,
        **kwargs,
    ) -> JSONResponse:
        """Get a specific item from a collection in a catalog.

        Args:
            catalog_id: The ID of the catalog.
            collection_id: The ID of the collection.
            item_id: The ID of the item.
            request: The FastAPI request object.
            **kwargs: Additional keyword arguments.

        Returns:
            JSONResponse containing the item.
        """
        item = await self.database.get_catalog_collection_item(
            catalog_id=catalog_id,
            collection_id=collection_id,
            item_id=item_id,
            request=request,
        )
        return JSONResponse(content=item)

    async def get_catalog_children(
        self,
        catalog_id: str,
        limit: int | None = None,
        token: str | None = None,
        request: Request | None = None,
        **kwargs,
    ) -> Children:
        """Get all children (catalogs and collections) of a catalog.

        Args:
            catalog_id: The ID of the catalog.
            limit: The maximum number of children to return.
            token: The pagination token.
            request: The FastAPI request object.
            **kwargs: Additional keyword arguments.

        Returns:
            Children object containing children list, total count, and pagination info.
        """
        # Check if offset is in query params (from pagination link)
        if request and not token:
            offset_param = request.query_params.get("offset")
            if offset_param:
                token = offset_param

        logger.info(f"get_catalog_children called with limit={limit}, token={token}")
        limit = limit or 10
        children_list, total_hits, _ = await self.database.get_catalog_children(
            catalog_id=catalog_id,
            limit=limit,
            token=token,
            request=request,
        )

        # Generate links dynamically for each child in scoped context
        if request and children_list:
            for child in children_list:
                child_id = cast(str, child.get("id"))
                child_type = child.get(
                    "type", "Catalog"
                )  # Default to Catalog if not specified
                parent_ids = child.get("parent_ids", [])

                # Generate inferred links using ChildLinks
                child["links"] = await ChildLinks(
                    catalog_id=catalog_id,
                    child_id=child_id,
                    child_type=child_type,
                    request=request,
                    parent_ids=parent_ids,
                ).get_links(extra_links=child.get("links"))

                # Remove internal metadata
                child.pop("parent_ids", None)

        # Generate pagination links - always generate from scratch based on offset
        # Don't rely on database's next_token as it may have empty body
        links = []
        if request:
            offset: int = _parse_pagination_token(token)

            # Check if there are more results
            next_token_to_use = None
            if total_hits and offset + len(children_list) < total_hits:
                # There are more results, generate next link
                next_offset = offset + len(children_list)
                next_token_to_use = {
                    "rel": "next",
                    "type": "application/json",
                    "body": {"offset": next_offset},
                }

            links = await CollectionSearchPagingLinks(
                request=request, next=next_token_to_use, prev=None
            ).get_links()

        return Children(
            children=children_list or [],
            links=links,
            numberMatched=total_hits,
            numberReturned=len(children_list) if children_list else 0,
        )

    async def get_catalog_conformance(
        self, catalog_id: str, request: Request | None = None, **kwargs
    ) -> JSONResponse:
        """Get conformance classes for a catalog.

        Args:
            catalog_id: The ID of the catalog.
            request: The FastAPI request object.
            **kwargs: Additional keyword arguments.

        Returns:
            JSONResponse containing conformance classes.

        Raises:
            NotFoundError: If the catalog does not exist.
        """
        # Validate that the catalog exists
        if request:
            await self.database.find_catalog(catalog_id, request=request)

        return JSONResponse(
            content={
                "conformsTo": [
                    "https://api.stacspec.org/v1.0.0/core",
                    "https://api.stacspec.org/v1.0.0-beta.4/multi-tenant-catalogs",
                    "https://api.stacspec.org/v1.0.0-beta.4/multi-tenant-catalogs/transaction",
                    "https://api.stacspec.org/v1.0.0-rc.2/children",
                ]
            }
        )

    async def get_catalog_queryables(
        self, catalog_id: str, request: Request | None = None, **kwargs
    ) -> JSONResponse:
        """Get queryables for a catalog.

        Args:
            catalog_id: The ID of the catalog.
            request: The FastAPI request object.
            **kwargs: Additional keyword arguments.

        Returns:
            JSONResponse containing queryables.

        Raises:
            NotFoundError: If the catalog does not exist.
        """
        # Validate that the catalog exists
        if request:
            await self.database.find_catalog(catalog_id, request=request)

        return JSONResponse(content={"queryables": []})

    async def unlink_sub_catalog(
        self,
        catalog_id: str,
        sub_catalog_id: str,
        request: Request | None = None,
        **kwargs,
    ) -> None:
        """Unlink a sub-catalog from its parent.

        Per spec: If the sub-catalog has no other parents after unlinking,
        it is automatically adopted by the Root Catalog.

        Args:
            catalog_id: The ID of the parent catalog.
            sub_catalog_id: The ID of the sub-catalog to unlink.
            request: The FastAPI request object.
            **kwargs: Additional keyword arguments.
        """
        await self.database.unlink_sub_catalog(
            catalog_id=catalog_id,
            sub_catalog_id=sub_catalog_id,
            request=request,
        )
