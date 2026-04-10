import json
import logging
from typing import Any

from buildpg import render
from stac_fastapi.types.errors import NotFoundError

from stac_fastapi.pgstac.db import dbfunc

logger = logging.getLogger(__name__)


class DatabaseLogic:
    """Database logic for catalogs extension using PGStac."""

    async def get_all_catalogs(
        self,
        token: str | None,
        limit: int,
        request: Any = None,
        sort: list[dict[str, Any]] | None = None,
    ) -> tuple[list[dict[str, Any]], str | None, int | None]:
        """Retrieve a list of catalogs from PGStac, supporting pagination.

        Args:
            token (str | None): The pagination token.
            limit (int): The number of results to return.
            request (Any, optional): The FastAPI request object. Defaults to None.
            sort (list[dict[str, Any]] | None, optional): Optional sort parameter. Defaults to None.

        Returns:
            A tuple of (catalogs, next pagination token if any, optional count).
        """
        if request is None:
            logger.debug("No request object provided to get_all_catalogs")
            return [], None, None

        try:
            async with request.app.state.get_connection(request, "r") as conn:
                logger.debug("Attempting to fetch all catalogs from database")
                q, p = render(
                    """
                    SELECT content
                    FROM collections
                    WHERE content->>'type' = 'Catalog'
                    ORDER BY id
                    LIMIT :limit OFFSET 0;
                    """,
                    limit=limit,
                )
                rows = await conn.fetch(q, *p)
                catalogs = [row[0] for row in rows] if rows else []
                logger.info(f"Successfully fetched {len(catalogs)} catalogs")
        except Exception as e:
            logger.warning(f"Error fetching all catalogs: {e}")
            catalogs = []

        return catalogs, None, len(catalogs) if catalogs else None

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

        try:
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
        except Exception:
            catalog = None

        if catalog is None:
            raise NotFoundError(f"Catalog {catalog_id} not found")

        return catalog

    async def create_catalog(
        self, catalog: dict[str, Any], refresh: bool = False, request: Any = None
    ) -> None:
        """Create or update a catalog.

        Args:
            catalog: The catalog dictionary.
            refresh: Whether to refresh after creation.
            request: The FastAPI request object.
        """
        if request is None:
            return

        try:
            async with request.app.state.get_connection(request, "w") as conn:
                await dbfunc(conn, "create_collection", dict(catalog))
        except Exception as e:
            logger.warning(f"Error creating catalog: {e}")

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
        except Exception as e:
            logger.warning(f"Error deleting catalog: {e}")

    async def get_catalog_children(
        self,
        catalog_id: str,
        limit: int = 10,
        token: str | None = None,
        request: Any = None,
    ) -> tuple[list[dict[str, Any]], int | None, str | None]:
        """Get all children (catalogs and collections) of a catalog.

        Args:
            catalog_id: The parent catalog ID.
            limit: The number of results to return.
            token: The pagination token.
            request: The FastAPI request object.

        Returns:
            A tuple of (children list, total count, next token).
        """
        if request is None:
            return [], None, None

        try:
            async with request.app.state.get_connection(request, "r") as conn:
                q, p = render(
                    """
                    SELECT content
                    FROM collections
                    WHERE content->'parent_ids' @> :parent_id::jsonb
                    ORDER BY content->>'type' DESC, id
                    LIMIT :limit OFFSET 0;
                    """,
                    parent_id=f'"{catalog_id}"',
                    limit=limit,
                )
                rows = await conn.fetch(q, *p)
                children = [row[0] for row in rows] if rows else []
        except Exception:
            children = []

        return children[:limit], len(children) if children else None, None

    async def get_catalog_collections(
        self,
        catalog_id: str,
        limit: int = 10,
        token: str | None = None,
        request: Any = None,
    ) -> tuple[list[dict[str, Any]], int | None, str | None]:
        """Get collections linked to a catalog.

        Args:
            catalog_id: The catalog ID.
            limit: The number of results to return.
            token: The pagination token.
            request: The FastAPI request object.

        Returns:
            A tuple of (collections list, total count, next token).
        """
        if request is None:
            return [], None, None

        try:
            async with request.app.state.get_connection(request, "r") as conn:
                q, p = render(
                    """
                    SELECT content
                    FROM collections
                    WHERE content->>'type' = 'Collection' AND content->'parent_ids' @> :parent_id::jsonb
                    ORDER BY id
                    LIMIT :limit OFFSET 0;
                    """,
                    parent_id=f'"{catalog_id}"',
                    limit=limit,
                )
                rows = await conn.fetch(q, *p)
                collections = [row[0] for row in rows] if rows else []
        except Exception:
            collections = []

        return collections[:limit], len(collections) if collections else None, None

    async def get_catalog_catalogs(
        self,
        catalog_id: str,
        limit: int = 10,
        token: str | None = None,
        request: Any = None,
    ) -> tuple[list[dict[str, Any]], int | None, str | None]:
        """Get sub-catalogs of a catalog.

        Args:
            catalog_id: The parent catalog ID.
            limit: The number of results to return.
            token: The pagination token.
            request: The FastAPI request object.

        Returns:
            A tuple of (catalogs list, total count, next token).
        """
        if request is None:
            return [], None, None

        try:
            async with request.app.state.get_connection(request, "r") as conn:
                q, p = render(
                    """
                    SELECT content
                    FROM collections
                    WHERE content->>'type' = 'Catalog' AND content->'parent_ids' @> :parent_id::jsonb
                    ORDER BY id
                    LIMIT :limit OFFSET 0;
                    """,
                    parent_id=f'"{catalog_id}"',
                    limit=limit,
                )
                rows = await conn.fetch(q, *p)
                catalogs = [row[0] for row in rows] if rows else []
        except Exception:
            catalogs = []

        return catalogs[:limit], len(catalogs) if catalogs else None, None

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
    ) -> None:
        """Create a collection.

        Args:
            collection: The collection dictionary.
            refresh: Whether to refresh after creation.
            request: The FastAPI request object.
        """
        if request is None:
            return

        try:
            async with request.app.state.get_connection(request, "w") as conn:
                await dbfunc(conn, "create_collection", dict(collection))
        except Exception as e:
            logger.warning(f"Error creating collection: {e}")

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

        async with request.app.state.get_connection(request, "w") as conn:
            q, p = render(
                """
                SELECT * FROM update_collection(:item::text::jsonb);
                """,
                item=json.dumps(collection),
            )
            await conn.fetchval(q, *p)

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

    async def get_catalog_collection_items(
        self,
        catalog_id: str,
        collection_id: str,
        bbox: Any = None,
        datetime: str | None = None,
        limit: int = 10,
        token: str | None = None,
        request: Any = None,
        **kwargs: Any,
    ) -> tuple[list[dict[str, Any]], int | None, str | None]:
        """Get items from a collection in a catalog.

        Args:
            catalog_id: The catalog ID.
            collection_id: The collection ID.
            bbox: Bounding box filter.
            datetime: Datetime filter.
            limit: The number of results to return.
            token: The pagination token.
            request: The FastAPI request object.
            **kwargs: Additional arguments.

        Returns:
            A tuple of (items list, total count, next token).
        """
        if request is None:
            return [], None, None

        async with request.app.state.get_connection(request, "r") as conn:
            q, p = render(
                """
                SELECT * FROM get_collection_items(:collection_id::text);
                """,
                collection_id=collection_id,
            )
            items = await conn.fetchval(q, *p) or []

        return items[:limit], len(items), None

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
