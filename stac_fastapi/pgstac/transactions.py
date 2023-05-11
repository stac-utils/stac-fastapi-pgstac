"""transactions extension client."""

import logging
import re
from typing import Optional, Union

import attr
from buildpg import render
from fastapi import HTTPException, Request
from stac_fastapi.extensions.third_party.bulk_transactions import (
    AsyncBaseBulkTransactionsClient,
    Items,
)
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.core import AsyncBaseTransactionsClient
from starlette.responses import JSONResponse, Response

from stac_fastapi.pgstac.db import dbfunc
from stac_fastapi.pgstac.models.links import CollectionLinks, ItemLinks
from stac_fastapi.pgstac.utils import INVALID_ID_CHARS

logger = logging.getLogger("uvicorn")
logger.setLevel(logging.INFO)

ID_REGEX = "[" + "".join(re.escape(char) for char in INVALID_ID_CHARS) + "]"


@attr.s
class TransactionsClient(AsyncBaseTransactionsClient):
    """Transactions extension specific CRUD operations."""

    def _validate_item(
        self,
        item: stac_types.Item,
        collection_id: str,
        expected_item_id: Optional[str] = None,
    ) -> None:
        """Validate item."""
        body_collection_id = item.get("collection")
        body_item_id = item.get("id")

        if body_collection_id is not None and collection_id != body_collection_id:
            raise HTTPException(
                status_code=400,
                detail=f"Collection ID from path parameter ({collection_id}) does not match Collection ID from Item ({body_collection_id})",
            )

        if bool(re.search(ID_REGEX, body_item_id)):
            raise HTTPException(
                status_code=400,
                detail=f"Item ID ({body_item_id}) cannot contain the following characters: {' '.join(INVALID_ID_CHARS)}",
            )

        if expected_item_id is not None and expected_item_id != body_item_id:
            raise HTTPException(
                status_code=400,
                detail=f"Item ID from path parameter ({expected_item_id}) does not match Item ID from Item ({body_item_id})",
            )

    async def create_item(
        self,
        collection_id: str,
        item: Union[stac_types.Item, stac_types.ItemCollection],
        request: Request,
        **kwargs,
    ) -> Optional[Union[stac_types.Item, Response]]:
        """Create item."""
        if item["type"] == "FeatureCollection":
            valid_items = []
            for item in item["features"]:
                self._validate_item(item, collection_id)
                item["collection"] = collection_id
                valid_items.append(item)

            async with request.app.state.get_connection(request, "w") as conn:
                await dbfunc(conn, "create_items", valid_items)
            return Response(status_code=201)

        self._validate_item(item, collection_id)
        item["collection"] = collection_id

        async with request.app.state.get_connection(request, "w") as conn:
            await dbfunc(conn, "create_item", item)

        item["links"] = await ItemLinks(
            collection_id=collection_id,
            item_id=item["id"],
            request=request,
        ).get_links(extra_links=item.get("links"))

        return stac_types.Item(**item)

    async def update_item(
        self,
        request: Request,
        collection_id: str,
        item_id: str,
        item: stac_types.Item,
        **kwargs,
    ) -> Optional[Union[stac_types.Item, Response]]:
        """Update item."""
        self._validate_item(item, collection_id, item_id)
        item["collection"] = collection_id

        async with request.app.state.get_connection(request, "w") as conn:
            await dbfunc(conn, "update_item", item)

        item["links"] = await ItemLinks(
            collection_id=collection_id,
            item_id=item["id"],
            request=request,
        ).get_links(extra_links=item.get("links"))

        return stac_types.Item(**item)

    async def create_collection(
        self, collection: stac_types.Collection, request: Request, **kwargs
    ) -> Optional[Union[stac_types.Collection, Response]]:
        """Create collection."""
        if bool(re.search(ID_REGEX, collection["id"])):
            raise HTTPException(
                status_code=400,
                detail=f"Collection ID ({collection['id']}) cannot contain the following characters: {' '.join(INVALID_ID_CHARS)}",
            )
        async with request.app.state.get_connection(request, "w") as conn:
            await dbfunc(conn, "create_collection", collection)
        collection["links"] = await CollectionLinks(
            collection_id=collection["id"], request=request
        ).get_links(extra_links=collection.get("links"))

        return stac_types.Collection(**collection)

    async def update_collection(
        self, collection: stac_types.Collection, request: Request, **kwargs
    ) -> Optional[Union[stac_types.Collection, Response]]:
        """Update collection."""
        async with request.app.state.get_connection(request, "w") as conn:
            await dbfunc(conn, "update_collection", collection)
        collection["links"] = await CollectionLinks(
            collection_id=collection["id"], request=request
        ).get_links(extra_links=collection.get("links"))
        return stac_types.Collection(**collection)

    async def delete_item(
        self, item_id: str, collection_id: str, request: Request, **kwargs
    ) -> Optional[Union[stac_types.Item, Response]]:
        """Delete item."""
        q, p = render(
            "SELECT * FROM delete_item(:item::text, :collection::text);",
            item=item_id,
            collection=collection_id,
        )
        async with request.app.state.get_connection(request, "w") as conn:
            await conn.fetchval(q, *p)
        return JSONResponse({"deleted item": item_id})

    async def delete_collection(
        self, collection_id: str, request: Request, **kwargs
    ) -> Optional[Union[stac_types.Collection, Response]]:
        """Delete collection."""
        async with request.app.state.get_connection(request, "w") as conn:
            await dbfunc(conn, "delete_collection", collection_id)
        return JSONResponse({"deleted collection": collection_id})


@attr.s
class BulkTransactionsClient(AsyncBaseBulkTransactionsClient):
    """Postgres bulk transactions."""

    async def bulk_item_insert(self, items: Items, request: Request, **kwargs) -> str:
        """Bulk item insertion using pgstac."""
        items = list(items.items.values())
        async with request.app.state.get_connection(request, "w") as conn:
            await dbfunc(conn, "create_items", items)

        return_msg = f"Successfully added {len(items)} items."
        return return_msg
