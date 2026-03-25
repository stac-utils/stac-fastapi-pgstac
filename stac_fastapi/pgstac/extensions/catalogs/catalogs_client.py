"""Catalogs client implementation for pgstac."""

import logging
from typing import Any, cast

import attr
from fastapi import Request
from stac_fastapi_catalogs_extension.client import AsyncBaseCatalogsClient
from stac_fastapi.types import stac as stac_types
from starlette.responses import JSONResponse

from stac_fastapi.types.errors import NotFoundError

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
        """Get all catalogs."""
        limit = limit or 10
        catalogs_list, next_token, total_hits = await self.database.get_all_catalogs(
            token=token,
            limit=limit,
            request=request,
        )

        return JSONResponse(
            content={
                "catalogs": catalogs_list or [],
                "links": [],
                "numberMatched": total_hits,
                "numberReturned": len(catalogs_list) if catalogs_list else 0,
            }
        )

    async def get_catalog(
        self, catalog_id: str, request: Request | None = None, **kwargs
    ) -> JSONResponse:
        """Get a specific catalog by ID."""
        try:
            catalog = await self.database.find_catalog(catalog_id, request=request)
            return JSONResponse(content=catalog)
        except NotFoundError:
            raise

    async def create_catalog(
        self, catalog: dict, request: Request | None = None, **kwargs
    ) -> stac_types.Catalog:
        """Create a new catalog."""
        # Convert Pydantic model to dict if needed
        catalog_dict = cast(stac_types.Catalog, catalog.model_dump(mode="json") if hasattr(catalog, "model_dump") else catalog)
        
        await self.database.create_catalog(dict(catalog_dict), refresh=True, request=request)
        return catalog_dict

    async def update_catalog(
        self, catalog_id: str, catalog: dict, request: Request | None = None, **kwargs
    ) -> stac_types.Catalog:
        """Update an existing catalog."""
        # Convert Pydantic model to dict if needed
        catalog_dict = cast(stac_types.Catalog, catalog.model_dump(mode="json") if hasattr(catalog, "model_dump") else catalog)
        
        await self.database.create_catalog(dict(catalog_dict), refresh=True, request=request)
        return catalog_dict

    async def delete_catalog(
        self, catalog_id: str, request: Request | None = None, **kwargs
    ) -> None:
        """Delete a catalog."""
        await self.database.delete_catalog(catalog_id, refresh=True, request=request)

    async def get_catalog_collections(
        self,
        catalog_id: str,
        limit: int | None = None,
        token: str | None = None,
        request: Request | None = None,
        **kwargs,
    ) -> JSONResponse:
        """Get collections in a catalog."""
        limit = limit or 10
        collections_list, total_hits, next_token = await self.database.get_catalog_collections(
            catalog_id=catalog_id,
            limit=limit,
            token=token,
            request=request,
        )
        return JSONResponse(
            content={
                "collections": collections_list or [],
                "links": [],
                "numberMatched": total_hits,
                "numberReturned": len(collections_list) if collections_list else 0,
            }
        )

    async def get_sub_catalogs(
        self,
        catalog_id: str,
        limit: int | None = None,
        token: str | None = None,
        request: Request | None = None,
        **kwargs,
    ) -> JSONResponse:
        """Get sub-catalogs."""
        limit = limit or 10
        catalogs_list, total_hits, next_token = await self.database.get_catalog_catalogs(
            catalog_id=catalog_id,
            limit=limit,
            token=token,
            request=request,
        )
        return JSONResponse(
            content={
                "catalogs": catalogs_list or [],
                "links": [],
                "numberMatched": total_hits,
                "numberReturned": len(catalogs_list) if catalogs_list else 0,
            }
        )

    async def create_sub_catalog(
        self, catalog_id: str, catalog: dict, request: Request | None = None, **kwargs
    ) -> JSONResponse:
        """Create a sub-catalog."""
        # Convert Pydantic model to dict if needed
        if hasattr(catalog, "model_dump"):
            catalog_dict = catalog.model_dump(mode="json")
        else:
            catalog_dict = dict(catalog) if not isinstance(catalog, dict) else catalog
        
        catalog_dict["parent_ids"] = [catalog_id]
        await self.database.create_catalog(catalog_dict, refresh=True, request=request)
        return JSONResponse(content=catalog_dict, status_code=201)

    async def create_catalog_collection(
        self, catalog_id: str, collection: dict, request: Request | None = None, **kwargs
    ) -> JSONResponse:
        """Create a collection in a catalog."""
        # Convert Pydantic model to dict if needed
        if hasattr(collection, "model_dump"):
            collection_dict = collection.model_dump(mode="json")
        else:
            collection_dict = dict(collection) if not isinstance(collection, dict) else collection
        
        collection_dict["parent_ids"] = [catalog_id]
        await self.database.create_collection(collection_dict, refresh=True, request=request)
        return JSONResponse(content=collection_dict, status_code=201)

    async def get_catalog_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request | None = None,
        **kwargs,
    ) -> JSONResponse:
        """Get a collection from a catalog."""
        collection = await self.database.get_catalog_collection(
            catalog_id=catalog_id,
            collection_id=collection_id,
            request=request,
        )
        return JSONResponse(content=collection)

    async def unlink_catalog_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request | None = None,
        **kwargs,
    ) -> None:
        """Unlink a collection from a catalog."""
        collection = await self.database.get_catalog_collection(
            catalog_id=catalog_id,
            collection_id=collection_id,
            request=request,
        )
        if "parent_ids" in collection:
            collection["parent_ids"] = [
                pid for pid in collection["parent_ids"] if pid != catalog_id
            ]
        await self.database.update_collection(
            collection_id, collection, refresh=True, request=request
        )

    async def get_catalog_collection_items(
        self,
        catalog_id: str,
        collection_id: str,
        limit: int | None = None,
        token: str | None = None,
        request: Request | None = None,
        **kwargs,
    ) -> JSONResponse:
        """Get items from a collection in a catalog."""
        limit = limit or 10
        items, total, next_token = await self.database.get_catalog_collection_items(
            catalog_id=catalog_id,
            collection_id=collection_id,
            limit=limit,
            token=token,
            request=request,
        )
        return JSONResponse(
            content={
                "type": "FeatureCollection",
                "features": items or [],
                "links": [],
                "numberMatched": total,
                "numberReturned": len(items) if items else 0,
            }
        )

    async def get_catalog_collection_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        request: Request | None = None,
        **kwargs,
    ) -> JSONResponse:
        """Get a specific item from a collection in a catalog."""
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
    ) -> JSONResponse:
        """Get all children of a catalog."""
        limit = limit or 10
        children_list, total_hits, next_token = await self.database.get_catalog_children(
            catalog_id=catalog_id,
            limit=limit,
            token=token,
            request=request,
        )
        return JSONResponse(
            content={
                "children": children_list or [],
                "links": [],
                "numberMatched": total_hits,
                "numberReturned": len(children_list) if children_list else 0,
            }
        )

    async def get_catalog_conformance(
        self, catalog_id: str, request: Request | None = None, **kwargs
    ) -> JSONResponse:
        """Get conformance classes for a catalog."""
        return JSONResponse(
            content={
                "conformsTo": [
                    "https://api.stacspec.org/v1.0.0/core",
                    "https://api.stacspec.org/v1.0.0/multi-tenant-catalogs",
                ]
            }
        )

    async def get_catalog_queryables(
        self, catalog_id: str, request: Request | None = None, **kwargs
    ) -> JSONResponse:
        """Get queryables for a catalog."""
        return JSONResponse(content={"queryables": []})

    async def unlink_sub_catalog(
        self,
        catalog_id: str,
        sub_catalog_id: str,
        request: Request | None = None,
        **kwargs,
    ) -> None:
        """Unlink a sub-catalog from its parent."""
        sub_catalog = await self.database.find_catalog(sub_catalog_id, request=request)
        if "parent_ids" in sub_catalog:
            sub_catalog["parent_ids"] = [
                pid for pid in sub_catalog["parent_ids"] if pid != catalog_id
            ]
        await self.database.create_catalog(sub_catalog, refresh=True, request=request)
