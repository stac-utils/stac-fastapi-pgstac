import json
import logging
from typing import Any
from urllib.parse import parse_qs, urlparse

from buildpg import render
from stac_fastapi.types.errors import NotFoundError

from stac_fastapi.pgstac.db import dbfunc

logger = logging.getLogger(__name__)


def _convert_pgstac_link_to_paging_link(link: dict[str, Any]) -> dict[str, Any]:
    """Convert PgSTAC link format to CollectionSearchPagingLinks format.

    PgSTAC returns links with href containing query parameters.
    CollectionSearchPagingLinks expects links with a body dict containing the parameters.

    Args:
        link: Link dict from PgSTAC with 'href' field

    Returns:
        Link dict with 'body' field containing parsed query parameters
    """
    href = link.get("href", "")
    parsed = urlparse(href)
    params = parse_qs(parsed.query)

    # parse_qs returns lists for values, convert to single values
    body = {
        k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in params.items()
    }

    return {
        "rel": link.get("rel"),
        "type": link.get("type"),
        "body": body,
    }


def _parse_pagination_token(token: str | None) -> int:
    """Parse pagination token to offset value.

    Args:
        token: Pagination token (plain integer or None)

    Returns:
        Offset value (0 if token is invalid)
    """
    if token:
        try:
            return int(token)
        except (ValueError, TypeError):
            return 0
    return 0


async def _execute_collection_search(
    conn: Any,
    search_query: dict[str, Any],
) -> tuple[list[dict[str, Any]], int | None, dict[str, Any] | None]:
    """Execute collection_search query and extract results and pagination.

    Args:
        conn: Database connection
        search_query: Search query dict with filter, limit, offset

    Returns:
        Tuple of (items list, total count, next link dict if any)
    """
    q, p = render(
        """
        SELECT * FROM collection_search(:search::text::jsonb);
        """,
        search=json.dumps(search_query),
    )
    result = await conn.fetchval(q, *p)
    items = result.get("collections", []) if result else []
    total_count = result.get("numberMatched") if result else None

    # Extract next link from result (PgSTAC returns pagination links)
    next_link = None
    if links := result.get("links"):
        for link in links:
            if link.get("rel") == "next":
                next_link = _convert_pgstac_link_to_paging_link(link)
                break

    return items, total_count, next_link


class CatalogsDatabaseLogic:
    """Database logic for catalogs extension using PGStac."""

    async def get_all_catalogs(
        self,
        token: str | None,
        limit: int,
        request: Any = None,
        sort: list[dict[str, Any]] | None = None,
    ) -> tuple[list[dict[str, Any]], int | None, dict[str, Any] | None]:
        """Retrieve all catalogs with pagination.

        Uses collection_search() pgSTAC function with CQL2 filters for API stability.

        Args:
            token: The pagination token.
            limit: The number of results to return.
            request: The FastAPI request object.
            sort: Optional sort parameter.

        Returns:
            A tuple of (catalogs list, total count, next link dict if any).
        """
        if request is None:
            logger.debug("No request object provided to get_all_catalogs")
            return [], None, None

        next_link = None
        total_count = None

        try:
            async with request.app.state.get_connection(request, "r") as conn:
                logger.debug("Attempting to fetch all catalogs from database")
                # Use collection_search with CQL2 filter for type='Catalog'
                # PgSTAC uses offset-based pagination for collections
                offset = _parse_pagination_token(token)

                search_query = {
                    "filter": {"op": "=", "args": [{"property": "type"}, "Catalog"]},
                    "limit": limit,
                    "offset": offset,
                }

                if sort:
                    search_query["sortby"] = sort

                catalogs, total_count, next_link = await _execute_collection_search(
                    conn, search_query
                )
                logger.info(f"Successfully fetched {len(catalogs)} catalogs")
        except (AttributeError, KeyError, TypeError) as e:
            logger.warning(f"Error parsing catalog search results: {e}")
            catalogs = []

        return catalogs, total_count, next_link

    async def find_catalog(self, catalog_id: str, request: Any = None) -> dict[str, Any]:
        """Find a catalog by ID.

        Args:
            catalog_id: The catalog ID to find.
            request: The FastAPI request object.

        Returns:
            The catalog dictionary.

        Raises:
            NotFoundError: If the catalog is not found.
        """
        if request is None:
            raise NotFoundError(f"Catalog {catalog_id} not found")

        async with request.app.state.get_connection(request, "r") as conn:
            q, p = render(
                """
                SELECT content
                FROM collections
                WHERE id = :id AND content->>'type' = 'Catalog';
                """,
                id=catalog_id,
            )
            row = await conn.fetchval(q, *p)
            catalog = row if row else None

        if catalog is None:
            raise NotFoundError(f"Catalog {catalog_id} not found")

        return catalog

    async def _check_cycle(
        self,
        catalog_id: str,
        parent_id: str,
        request: Any = None,
    ) -> bool:
        """Check if adding parent_id to catalog_id would create a cycle.

        Args:
            catalog_id: The catalog being linked.
            parent_id: The proposed parent catalog ID.
            request: The FastAPI request object.

        Returns:
            True if a cycle would be created, False otherwise.
        """
        if request is None:
            return False

        if catalog_id == parent_id:
            return True

        try:
            # Get the parent catalog
            parent = await self.find_catalog(parent_id, request=request)
            parent_ids = parent.get("parent_ids", [])

            # If parent has catalog_id as a parent, it's a cycle
            if catalog_id in parent_ids:
                return True

            # Recursively check parent's parents
            for pid in parent_ids:
                if await self._check_cycle(catalog_id, pid, request):
                    return True
        except NotFoundError:
            pass

        return False

    async def create_catalog(
        self, catalog: dict[str, Any], refresh: bool = False, request: Any = None
    ) -> bool:
        """Create or update a catalog.

        Args:
            catalog: The catalog dictionary.
            refresh: Whether to refresh after creation.
            request: The FastAPI request object.

        Returns:
            True if creation was successful, False otherwise.
        """
        if request is None:
            return False

        try:
            async with request.app.state.get_connection(request, "w") as conn:
                await dbfunc(conn, "create_collection", dict(catalog))
            return True
        except Exception as e:
            logger.error(
                f"Conflict or database error creating catalog: {e}", exc_info=True
            )
            raise

    async def update_catalog(
        self,
        catalog_id: str,
        catalog: dict[str, Any],
        refresh: bool = False,
        request: Any = None,
    ) -> None:
        """Update a catalog's metadata.

        Per spec: This operation MUST NOT modify the structural links (parent_ids)
        of the catalog unless explicitly handled, ensuring the catalog remains
        in its current hierarchy.

        Args:
            catalog_id: The catalog ID to update.
            catalog: The updated catalog dictionary.
            refresh: Whether to refresh after update.
            request: The FastAPI request object.
        """
        if request is None:
            return

        try:
            # Get existing catalog to preserve parent_ids
            existing = await self.find_catalog(catalog_id, request=request)

            # Only preserve parent_ids if not explicitly provided in the update
            if "parent_ids" not in catalog:
                catalog["parent_ids"] = existing.get("parent_ids", [])

            # Merge with existing data
            catalog["id"] = catalog_id

            async with request.app.state.get_connection(request, "w") as conn:
                q, p = render(
                    """
                    SELECT * FROM update_collection(:item::text::jsonb);
                    """,
                    item=json.dumps(catalog),
                )
                await conn.fetchval(q, *p)
            logger.info(f"Successfully updated catalog {catalog_id}")
        except Exception as e:
            logger.error(f"Error updating catalog {catalog_id}: {e}", exc_info=True)
            raise

    async def delete_catalog(
        self, catalog_id: str, refresh: bool = False, request: Any = None
    ) -> None:
        """Delete a catalog.

        Args:
            catalog_id: The catalog ID to delete.
            refresh: Whether to refresh after deletion.
            request: The FastAPI request object.
        """
        if request is None:
            return

        try:
            async with request.app.state.get_connection(request, "w") as conn:
                await dbfunc(conn, "delete_collection", catalog_id)
            logger.info(f"Successfully deleted catalog {catalog_id}")
        except Exception as e:
            logger.error(f"Error deleting catalog {catalog_id}: {e}", exc_info=True)
            raise

    async def get_catalog_children(
        self,
        catalog_id: str,
        limit: int = 10,
        token: str | None = None,
        request: Any = None,
        sort: list[dict[str, Any]] | None = None,
    ) -> tuple[list[dict[str, Any]], int | None, dict[str, Any] | None]:
        """Get all children (catalogs and collections) of a catalog.

        Uses collection_search() pgSTAC function with CQL2 filters for API stability.

        Args:
            catalog_id: The parent catalog ID.
            limit: The number of results to return.
            token: The pagination token.
            request: The FastAPI request object.

        Returns:
            A tuple of (children list, total count, next link dict if any).
        """
        if request is None:
            return [], None, None

        # Validate parent catalog exists
        try:
            await self.find_catalog(catalog_id, request=request)
        except NotFoundError:
            raise

        next_link = None
        total_count = None

        try:
            async with request.app.state.get_connection(request, "r") as conn:
                # Use collection_search with CQL2 filter for parent_ids contains catalog_id
                # No type filter needed - returns both Catalogs and Collections
                # PgSTAC uses offset-based pagination for collections
                offset = _parse_pagination_token(token)

                search_query = {
                    "filter": {
                        "op": "a_contains",
                        "args": [{"property": "parent_ids"}, catalog_id],
                    },
                    "limit": limit,
                    "offset": offset,
                }

                if sort:
                    search_query["sortby"] = sort

                children, total_count, next_link = await _execute_collection_search(
                    conn, search_query
                )
        except (AttributeError, KeyError, TypeError) as e:
            logger.warning(f"Error parsing catalog children results: {e}")
            children = []

        return children, total_count, next_link

    async def get_catalog_collections(
        self,
        catalog_id: str,
        limit: int = 10,
        token: str | None = None,
        request: Any = None,
        sort: list[dict[str, Any]] | None = None,
    ) -> tuple[list[dict[str, Any]], int | None, dict[str, Any] | None]:
        """Get collections linked to a catalog.

        Uses collection_search() pgSTAC function with CQL2 filters for API stability.

        Args:
            catalog_id: The catalog ID.
            limit: The number of results to return.
            token: The pagination token.
            request: The FastAPI request object.

        Returns:
            A tuple of (collections list, total count, next link dict if any).
        """
        if request is None:
            return [], None, None

        # Validate parent catalog exists
        try:
            await self.find_catalog(catalog_id, request=request)
        except NotFoundError:
            raise

        next_link = None
        total_count = None

        try:
            async with request.app.state.get_connection(request, "r") as conn:
                # Use collection_search with CQL2 filter for type='Collection' and parent_ids contains catalog_id
                # Using 'a_contains' (Array Contains) operator to check if catalog_id is in the parent_ids array
                # PgSTAC uses offset-based pagination for collections
                offset = _parse_pagination_token(token)

                search_query = {
                    "filter": {
                        "op": "and",
                        "args": [
                            {"op": "=", "args": [{"property": "type"}, "Collection"]},
                            {
                                "op": "a_contains",
                                "args": [{"property": "parent_ids"}, catalog_id],
                            },
                        ],
                    },
                    "limit": limit,
                    "offset": offset,
                }

                if sort:
                    search_query["sortby"] = sort

                collections, total_count, next_link = await _execute_collection_search(
                    conn, search_query
                )
        except (AttributeError, KeyError, TypeError) as e:
            logger.warning(f"Error parsing catalog collections results: {e}")
            collections = []

        return collections, total_count, next_link

    async def get_sub_catalogs(
        self,
        catalog_id: str,
        limit: int = 10,
        token: str | None = None,
        request: Any = None,
        sort: list[dict[str, Any]] | None = None,
    ) -> tuple[list[dict[str, Any]], int | None, dict[str, Any] | None]:
        """Get sub-catalogs of a catalog.

        Uses collection_search() pgSTAC function with CQL2 filters for API stability.

        Args:
            catalog_id: The parent catalog ID.
            limit: The number of results to return.
            token: The pagination token.
            request: The FastAPI request object.

        Returns:
            A tuple of (catalogs list, total count, next link dict if any).
        """
        if request is None:
            return [], None, None

        # Validate parent catalog exists
        try:
            await self.find_catalog(catalog_id, request=request)
        except NotFoundError:
            raise

        next_link = None
        total_count = None

        try:
            async with request.app.state.get_connection(request, "r") as conn:
                logger.debug(f"Fetching sub-catalogs for parent: {catalog_id}")
                # Use collection_search with CQL2 filter for type='Catalog' and parent_ids contains catalog_id
                # Using 'a_contains' (Array Contains) operator to check if catalog_id is in the parent_ids array
                # PgSTAC uses offset-based pagination for collections
                offset = _parse_pagination_token(token)

                search_query = {
                    "filter": {
                        "op": "and",
                        "args": [
                            {"op": "=", "args": [{"property": "type"}, "Catalog"]},
                            {
                                "op": "a_contains",
                                "args": [{"property": "parent_ids"}, catalog_id],
                            },
                        ],
                    },
                    "limit": limit,
                    "offset": offset,
                }

                if sort:
                    search_query["sortby"] = sort

                catalogs, total_count, next_link = await _execute_collection_search(
                    conn, search_query
                )
                logger.debug(f"Found {len(catalogs)} sub-catalogs")
        except (AttributeError, KeyError, TypeError) as e:
            logger.warning(f"Error parsing sub-catalogs results: {e}")
            catalogs = []

        return catalogs, total_count, next_link

    async def find_collection(
        self, collection_id: str, request: Any = None
    ) -> dict[str, Any]:
        """Find a collection by ID.

        Args:
            collection_id: The collection ID to find.
            request: The FastAPI request object.

        Returns:
            The collection dictionary.

        Raises:
            NotFoundError: If the collection is not found.
        """
        if request is None:
            raise NotFoundError(f"Collection {collection_id} not found")

        async with request.app.state.get_connection(request, "r") as conn:
            q, p = render(
                """
                SELECT * FROM get_collection(:id::text);
                """,
                id=collection_id,
            )
            collection = await conn.fetchval(q, *p)

        if collection is None:
            raise NotFoundError(f"Collection {collection_id} not found")

        return collection

    async def create_collection(
        self, collection: dict[str, Any], refresh: bool = False, request: Any = None
    ) -> bool:
        """Create a collection.

        Args:
            collection: The collection dictionary.
            refresh: Whether to refresh after creation.
            request: The FastAPI request object.

        Returns:
            True if creation was successful, False otherwise.
        """
        if request is None:
            return False

        try:
            async with request.app.state.get_connection(request, "w") as conn:
                await dbfunc(conn, "create_collection", dict(collection))
            return True
        except Exception as e:
            logger.error(
                f"Conflict or database error creating collection: {e}", exc_info=True
            )
            raise

    async def update_collection(
        self,
        collection_id: str,
        collection: dict[str, Any],
        refresh: bool = False,
        request: Any = None,
    ) -> None:
        """Update a collection.

        Args:
            collection_id: The collection ID to update.
            collection: The collection dictionary.
            refresh: Whether to refresh after update.
            request: The FastAPI request object.
        """
        if request is None:
            return

        try:
            async with request.app.state.get_connection(request, "w") as conn:
                q, p = render(
                    """
                    SELECT * FROM update_collection(:item::text::jsonb);
                    """,
                    item=json.dumps(collection),
                )
                await conn.fetchval(q, *p)
        except Exception as e:
            logger.error(f"Error updating collection {collection_id}: {e}", exc_info=True)
            raise

    async def update_catalog_collection(
        self,
        catalog_id: str,
        collection_id: str,
        collection: dict[str, Any],
        request: Any = None,
    ) -> dict[str, Any]:
        """Update a collection while ensuring parent_ids are preserved.

        Args:
            catalog_id: The catalog ID.
            collection_id: The collection ID to update.
            collection: The collection dictionary.
            request: The FastAPI request object.

        Returns:
            The updated collection dictionary.

        Raises:
            Exception: If the update fails.
        """
        if request is None:
            return collection

        try:
            # 1. Fetch existing collection to extract current DAG state
            existing = await self.find_collection(collection_id, request=request)
            parent_ids = existing.get("parent_ids", [])

            # 2. Re-inject parent_ids and ensure IDs match
            collection["id"] = collection_id
            collection["parent_ids"] = parent_ids

            # 3. Execute the standard pgSTAC update function
            async with request.app.state.get_connection(request, "w") as conn:
                q, p = render(
                    """
                    SELECT * FROM update_collection(:item::text::jsonb);
                    """,
                    item=json.dumps(collection),
                )
                await conn.fetchval(q, *p)

            logger.info(f"Successfully updated catalog collection {collection_id}")
            return collection
        except Exception as e:
            logger.error(f"Error updating catalog collection: {e}", exc_info=True)
            raise

    async def get_catalog_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Any = None,
    ) -> dict[str, Any]:
        """Get a specific collection from a catalog.

        Args:
            catalog_id: The catalog ID.
            collection_id: The collection ID.
            request: The FastAPI request object.

        Returns:
            The collection dictionary.

        Raises:
            NotFoundError: If the collection is not found or not linked to the catalog.
        """
        if request is None:
            raise NotFoundError(f"Collection {collection_id} not found")

        # Verify catalog exists
        try:
            await self.find_catalog(catalog_id, request=request)
        except NotFoundError as e:
            raise NotFoundError(f"Catalog {catalog_id} not found") from e

        async with request.app.state.get_connection(request, "r") as conn:
            q, p = render(
                """
                SELECT * FROM get_collection(:id::text);
                """,
                id=collection_id,
            )
            collection = await conn.fetchval(q, *p)

        if collection is None:
            raise NotFoundError(f"Collection {collection_id} not found")

        # Verify collection is linked to this catalog
        parent_ids = collection.get("parent_ids", [])
        if catalog_id not in parent_ids:
            raise NotFoundError(
                f"Collection {collection_id} not found in catalog {catalog_id}"
            )

        return collection

    async def get_catalog_collection_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        request: Any = None,
    ) -> dict[str, Any]:
        """Get a specific item from a collection in a catalog.

        Args:
            catalog_id: The catalog ID.
            collection_id: The collection ID.
            item_id: The item ID.
            request: The FastAPI request object.

        Returns:
            The item dictionary.

        Raises:
            NotFoundError: If the item is not found.
        """
        if request is None:
            raise NotFoundError(f"Item {item_id} not found")

        async with request.app.state.get_connection(request, "r") as conn:
            q, p = render(
                """
                SELECT * FROM get_item(:item_id::text, :collection_id::text);
                """,
                item_id=item_id,
                collection_id=collection_id,
            )
            item = await conn.fetchval(q, *p)

        if item is None:
            raise NotFoundError(f"Item {item_id} not found")

        return item

    async def unlink_sub_catalog(
        self,
        catalog_id: str,
        sub_catalog_id: str,
        request: Any = None,
    ) -> None:
        """Unlink a sub-catalog from its parent.

        Per spec: If the sub-catalog has no other parents after unlinking,
        it MUST be automatically adopted by the Root Catalog.

        Args:
            catalog_id: The parent catalog ID.
            sub_catalog_id: The sub-catalog ID to unlink.
            request: The FastAPI request object.

        Raises:
            NotFoundError: If the sub-catalog is not a child of the parent catalog.
        """
        if request is None:
            return

        try:
            # Get the sub-catalog
            sub_catalog = await self.find_catalog(sub_catalog_id, request=request)
            parent_ids = sub_catalog.get("parent_ids", [])

            # Check if catalog_id is in parent_ids
            if catalog_id not in parent_ids:
                raise NotFoundError(
                    f"Catalog {sub_catalog_id} is not a child of {catalog_id}"
                )

            # Remove the parent from parent_ids
            parent_ids = [p for p in parent_ids if p != catalog_id]

            # If no other parents, adopt to root (empty parent_ids means root)
            sub_catalog["parent_ids"] = parent_ids

            # Update the catalog using direct SQL to preserve parent_ids changes
            async with request.app.state.get_connection(request, "w") as conn:
                q, p = render(
                    """
                    SELECT * FROM update_collection(:item::text::jsonb);
                    """,
                    item=json.dumps(sub_catalog),
                )
                await conn.fetchval(q, *p)
            logger.info(f"Unlinked sub-catalog {sub_catalog_id} from parent {catalog_id}")
        except Exception as e:
            logger.error(f"Error unlinking sub-catalog: {e}", exc_info=True)
            raise

    async def unlink_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Any = None,
    ) -> None:
        """Unlink a collection from a catalog.

        Per spec: If the collection has no other parents after unlinking,
        it MUST be automatically adopted by the Root Catalog.

        Args:
            catalog_id: The parent catalog ID.
            collection_id: The collection ID to unlink.
            request: The FastAPI request object.

        Raises:
            NotFoundError: If the collection is not linked to the parent catalog.
        """
        if request is None:
            return

        try:
            # Get the collection
            collection = await self.find_collection(collection_id, request=request)
            parent_ids = collection.get("parent_ids", [])

            # Check if catalog_id is in parent_ids
            if catalog_id not in parent_ids:
                raise NotFoundError(
                    f"Collection {collection_id} is not linked to catalog {catalog_id}"
                )

            # Remove the parent from parent_ids
            parent_ids = [p for p in parent_ids if p != catalog_id]

            # If no other parents, adopt to root (empty parent_ids means root)
            collection["parent_ids"] = parent_ids

            # Update the collection using direct SQL to preserve parent_ids changes
            async with request.app.state.get_connection(request, "w") as conn:
                q, p = render(
                    """
                    SELECT * FROM update_collection(:item::text::jsonb);
                    """,
                    item=json.dumps(collection),
                )
                await conn.fetchval(q, *p)
            logger.info(f"Unlinked collection {collection_id} from catalog {catalog_id}")
        except Exception as e:
            logger.error(f"Error unlinking collection: {e}", exc_info=True)
            raise
