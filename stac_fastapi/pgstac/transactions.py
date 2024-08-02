"""transactions extension client."""

import logging
import re
from typing import Optional, Union

import attr
from buildpg import render
from fastapi import HTTPException, Request
from stac_fastapi.extensions.third_party.bulk_transactions import (
    AsyncBaseBulkTransactionsClient,
    BulkTransactionMethod,
    Items,
)
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.core import AsyncBaseTransactionsClient
from stac_pydantic import Collection, Item, ItemCollection
from starlette.responses import JSONResponse, Response

from stac_fastapi.pgstac.config import Settings
from stac_fastapi.pgstac.db import dbfunc
from stac_fastapi.pgstac.models.links import CollectionLinks, ItemLinks

logger = logging.getLogger("uvicorn")
logger.setLevel(logging.INFO)


class ClientValidateMixIn:
    def _validate_id(self, id: str, settings: Settings):
        invalid_chars = settings.invalid_id_chars
        id_regex = "[" + "".join(re.escape(char) for char in invalid_chars) + "]"

        if bool(re.search(id_regex, id)):
            raise HTTPException(
                status_code=400,
                detail=f"ID ({id}) cannot contain the following characters: {' '.join(invalid_chars)}",
            )

    def _validate_collection(self, request: Request, collection: stac_types.Collection):
        self._validate_id(collection["id"], request.app.state.settings)

    def _validate_item(
        self,
        request: Request,
        item: stac_types.Item,
        collection_id: str,
        expected_item_id: Optional[str] = None,
    ) -> None:
        """Validate item."""
        body_collection_id = item.get("collection")
        body_item_id = item.get("id")

        self._validate_id(body_item_id, request.app.state.settings)

        if body_collection_id is not None and collection_id != body_collection_id:
            raise HTTPException(
                status_code=400,
                detail=f"Collection ID from path parameter ({collection_id}) does not match Collection ID from Item ({body_collection_id})",
            )

        if expected_item_id is not None and expected_item_id != body_item_id:
            raise HTTPException(
                status_code=400,
                detail=f"Item ID from path parameter ({expected_item_id}) does not match Item ID from Item ({body_item_id})",
            )


@attr.s
class TransactionsClient(AsyncBaseTransactionsClient, ClientValidateMixIn):
    """Transactions extension specific CRUD operations."""

    async def create_item(
        self,
        collection_id: str,
        item: Union[Item, ItemCollection],
        request: Request,
        **kwargs,
    ) -> Optional[Union[stac_types.Item, Response]]:
        """Create item."""
        item = item.model_dump(mode="json")

        if item["type"] == "FeatureCollection":
            valid_items = []
            for item in item["features"]:  # noqa: B020
                self._validate_item(request, item, collection_id)
                item["collection"] = collection_id
                valid_items.append(item)

            async with request.app.state.get_connection(request, "w") as conn:
                await dbfunc(conn, "create_items", valid_items)

            return Response(status_code=201)

        elif item["type"] == "Feature":
            self._validate_item(request, item, collection_id)
            item["collection"] = collection_id

            async with request.app.state.get_connection(request, "w") as conn:
                await dbfunc(conn, "create_item", item)

            item["links"] = await ItemLinks(
                collection_id=collection_id,
                item_id=item["id"],
                request=request,
            ).get_links(extra_links=item.get("links"))

            return stac_types.Item(**item)

        else:
            raise HTTPException(
                status_code=400,
                detail=f"Item body type must be 'Feature' or 'FeatureCollection', not {item['type']}",
            )

    async def update_item(
        self,
        request: Request,
        collection_id: str,
        item_id: str,
        item: Item,
        **kwargs,
    ) -> Optional[Union[stac_types.Item, Response]]:
        """Update item."""
        item = item.model_dump(mode="json")

        self._validate_item(request, item, collection_id, item_id)
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
        self,
        collection: Collection,
        request: Request,
        **kwargs,
    ) -> Optional[Union[stac_types.Collection, Response]]:
        """Create collection."""
        collection = collection.model_dump(mode="json")

        self._validate_collection(request, collection)

        async with request.app.state.get_connection(request, "w") as conn:
            await dbfunc(conn, "create_collection", collection)

        collection["links"] = await CollectionLinks(
            collection_id=collection["id"], request=request
        ).get_links(extra_links=collection["links"])

        return stac_types.Collection(**collection)

    async def update_collection(
        self,
        collection: Collection,
        request: Request,
        **kwargs,
    ) -> Optional[Union[stac_types.Collection, Response]]:
        """Update collection."""

        col = collection.model_dump(mode="json")

        async with request.app.state.get_connection(request, "w") as conn:
            await dbfunc(conn, "update_collection", col)

        col["links"] = await CollectionLinks(
            collection_id=col["id"], request=request
        ).get_links(extra_links=col.get("links"))

        return stac_types.Collection(**col)

    async def delete_item(
        self,
        item_id: str,
        collection_id: str,
        request: Request,
        **kwargs,
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
class BulkTransactionsClient(AsyncBaseBulkTransactionsClient, ClientValidateMixIn):
    """Postgres bulk transactions."""

    async def bulk_item_insert(self, items: Items, request: Request, **kwargs) -> str:
        """Bulk item insertion using pgstac."""
        collection_id = request.path_params["collection_id"]

        for item_id, item in items.items.items():
            self._validate_item(request, item, collection_id, item_id)
            item["collection"] = collection_id

        items_to_insert = list(items.items.values())

        async with request.app.state.get_connection(request, "w") as conn:
            if items.method == BulkTransactionMethod.INSERT:
                method_verb = "added"
                await dbfunc(conn, "create_items", items_to_insert)
            elif items.method == BulkTransactionMethod.UPSERT:
                method_verb = "upserted"
                await dbfunc(conn, "upsert_items", items_to_insert)

        return_msg = f"Successfully {method_verb} {len(items_to_insert)} items."
        return return_msg
