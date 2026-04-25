"""Item crud client."""

import json
import re
from typing import Any
from urllib.parse import unquote_plus, urljoin

import attr
import orjson
from asyncpg.exceptions import InvalidDatetimeFormatError
from buildpg import render
from cql2 import Expr
from fastapi import HTTPException, Request
from hydraters import hydrate
from pydantic import ValidationError
from stac_fastapi.api.models import JSONResponse
from stac_fastapi.types.core import AsyncBaseCoreClient, Relations
from stac_fastapi.types.errors import InvalidQueryParameter, NotFoundError
from stac_fastapi.types.requests import get_base_url
from stac_fastapi.types.stac import Collection, Collections, Item, ItemCollection
from stac_pydantic.shared import BBox, MimeTypes

from stac_fastapi.pgstac.config import Settings
from stac_fastapi.pgstac.models.links import (
    CollectionLinks,
    CollectionSearchPagingLinks,
    ItemCollectionLinks,
    ItemLinks,
    PagingLinks,
    SearchLinks,
)
from stac_fastapi.pgstac.types.search import PgstacSearch
from stac_fastapi.pgstac.utils import filter_fields

NumType = float | int


@attr.s
class CoreCrudClient(AsyncBaseCoreClient):
    """Client for core endpoints defined by stac."""

    pgstac_search_model: type[PgstacSearch] = attr.ib(default=PgstacSearch)

    async def all_collections(  # type: ignore [override] # noqa: C901
        self,
        request: Request,
        # Extensions
        bbox: BBox | None = None,
        datetime: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        query: str | None = None,
        fields: list[str] | None = None,
        sortby: str | None = None,
        filter_expr: str | None = None,
        filter_lang: str | None = None,
        **kwargs: Any,
    ) -> Collections:
        """Cross catalog search (GET).

        Called with `GET /collections`.

        Returns:
            Collections which match the search criteria, returns all
            collections by default.
        """
        base_url = get_base_url(request)

        next_link: dict[str, Any] | None = None
        prev_link: dict[str, Any] | None = None
        collections: Collections

        if self.extension_is_enabled("CollectionSearchExtension"):
            base_args = {
                "bbox": bbox,
                "limit": limit,
                "offset": offset,
                "query": orjson.loads(unquote_plus(query)) if query else query,
            }

            clean_args = self._clean_search_args(
                base_args=base_args,
                datetime=datetime,
                fields=fields,
                sortby=sortby,
                filter_query=filter_expr,
                filter_lang=filter_lang,
                **kwargs,
            )

            # NOTE: `FreeTextExtension` - pgstac will only accept `str` so we need to
            # join the list[str] with ` OR `
            # ref: https://github.com/stac-utils/stac-fastapi-pgstac/pull/263
            if q := clean_args.pop("q", None):
                clean_args["q"] = " OR ".join(q) if isinstance(q, list) else q

            async with request.app.state.get_connection(request, "r") as conn:
                q, p = render(
                    """
                    SELECT * FROM collection_search(:req::text::jsonb);
                    """,
                    req=json.dumps(clean_args),
                )
                collections = await conn.fetchval(q, *p)

            if links := collections.get("links"):
                for link in links:
                    if link["rel"] == "next":
                        next_link = link
                    elif link["rel"] == "prev":
                        prev_link = link

        else:
            async with request.app.state.get_connection(request, "r") as conn:
                cols: list[Collection] = (
                    await conn.fetchval(
                        """
                    SELECT * FROM all_collections();
                    """
                    )
                    or []
                )

                collections = Collections(collections=cols, links=[])

        for collection in collections["collections"]:
            collection["links"] = await CollectionLinks(
                collection_id=collection["id"], request=request
            ).get_links(extra_links=collection.get("links"))

            if self.extension_is_enabled("FilterExtension") or self.extension_is_enabled(
                "ItemCollectionFilterExtension"
            ):
                collection["links"].append(
                    {
                        "rel": Relations.queryables.value,
                        "type": MimeTypes.jsonschema.value,
                        "title": "Queryables",
                        "href": urljoin(
                            base_url, f"collections/{collection['id']}/queryables"
                        ),
                    }
                )

        collections["links"] = await CollectionSearchPagingLinks(
            request=request, next=next_link, prev=prev_link
        ).get_links()

        # Make sure Collections Body has numberMatched and numberReturned
        total_collections = len(collections["collections"])
        collections["numberMatched"] = collections.get("numberMatched", total_collections)
        collections["numberReturned"] = collections.get(
            "numberReturned", total_collections
        )

        # If we have the `fields` extension enabled
        # we need to avoid Pydantic validation because the
        # Items might not be a valid STAC Item objects
        if fields:
            return JSONResponse(collections)  # type: ignore

        return collections

    async def get_collection(  # type: ignore [override]
        self,
        collection_id: str,
        request: Request,
        **kwargs: Any,
    ) -> Collection:
        """Get collection by id.

        Called with `GET /collections/{collection_id}`.

        Args:
            collection_id: ID of the collection.

        Returns:
            Collection.
        """
        async with request.app.state.get_connection(request, "r") as conn:
            q, p = render(
                """
                SELECT * FROM get_collection(:id::text);
                """,
                id=collection_id,
            )
            collection: Collection | None = await conn.fetchval(q, *p)

        if collection is None:
            raise NotFoundError(f"Collection {collection_id} does not exist.")

        collection["links"] = await CollectionLinks(
            collection_id=collection_id, request=request
        ).get_links(extra_links=collection.get("links"))

        if self.extension_is_enabled("FilterExtension") or self.extension_is_enabled(
            "ItemCollectionFilterExtension"
        ):
            base_url = get_base_url(request)
            collection["links"].append(
                {
                    "rel": Relations.queryables.value,
                    "type": MimeTypes.jsonschema.value,
                    "title": "Queryables",
                    "href": urljoin(base_url, f"collections/{collection_id}/queryables"),
                }
            )

        return collection

    async def _get_base_item(
        self,
        collection_id: str,
        request: Request,
    ) -> dict[str, Any]:
        """Get the base item of a collection for use in rehydrating full item collection properties.

        Args:
            collection_id: ID of the collection.

        Returns:
            Item.
        """
        item: dict[str, Any] | None

        async with request.app.state.get_connection(request, "r") as conn:
            q, p = render(
                """
                SELECT * FROM collection_base_item(:collection_id::text);
                """,
                collection_id=collection_id,
            )
            item = await conn.fetchval(q, *p)

        if item is None:
            raise NotFoundError(f"A base item for {collection_id} does not exist.")

        return item

    async def _search_base(  # noqa: C901  # type: ignore [override]
        self,
        search_request: PgstacSearch,
        request: Request,
    ) -> ItemCollection:
        """Cross catalog search (POST).

        Called with `POST /search`.

        Args:
            search_request: search request parameters.

        Returns:
            ItemCollection containing items which match the search criteria.
        """
        settings: Settings = request.app.state.settings

        search_request.conf = search_request.conf or {}
        search_request.conf["nohydrate"] = settings.use_api_hydrate

        search_request_json = search_request.model_dump_json(
            exclude_none=True, by_alias=True
        )

        try:
            async with request.app.state.get_connection(request, "r") as conn:
                q, p = render(
                    """
                    SELECT * FROM search(:req::text::jsonb);
                    """,
                    req=search_request_json,
                )
                item_collection: ItemCollection = await conn.fetchval(q, *p)

        except InvalidDatetimeFormatError as e:
            raise InvalidQueryParameter(
                f"Datetime parameter {search_request.datetime} is invalid."
            ) from e

        # Starting in pgstac 0.9.0, the `next` and `prev` tokens are returned in spec-compliant links with method GET
        next_from_link: str | None = None
        prev_from_link: str | None = None

        for link in item_collection.get("links", []):
            if link.get("rel") == "next":
                next_from_link = link["href"].split("token=next:")[1]
            if link.get("rel") == "prev":
                prev_from_link = link["href"].split("token=prev:")[1]

        # NOTE: Old version of pgstac returned `next` and `prev` links directly in the response
        next: str | None = item_collection.pop("next", next_from_link)  # type: ignore [typeddict-item]
        prev: str | None = item_collection.pop("prev", prev_from_link)  # type: ignore [typeddict-item]

        fields = getattr(search_request, "fields", None)
        include: set[str] = fields.include if fields and fields.include else set()
        exclude: set[str] = fields.exclude if fields and fields.exclude else set()

        async def _add_item_links(
            feature: Item,
            collection_id: str | None = None,
            item_id: str | None = None,
        ) -> None:
            """Add ItemLinks to the Item.

            If the fields extension is excluding links, then don't add them.
            Also skip links if the item doesn't provide collection and item ids.
            """
            collection_id = feature.get("collection") or collection_id
            item_id = feature.get("id") or item_id

            if not exclude or "links" not in exclude and all([collection_id, item_id]):
                feature["links"] = await ItemLinks(
                    collection_id=collection_id,  # type: ignore
                    item_id=item_id,  # type: ignore
                    request=request,
                ).get_links(extra_links=feature.get("links"))

        items: list[Item] = []
        if settings.use_api_hydrate:

            async def _get_base_item(collection_id: str) -> dict[str, Any]:
                return await self._get_base_item(collection_id, request=request)

            base_item_cache = settings.base_item_cache(
                fetch_base_item=_get_base_item, request=request
            )

            for item in item_collection.get("features", []):
                base_item = await base_item_cache.get(item.get("collection"))
                # Exclude None values
                base_item = {k: v for k, v in base_item.items() if v is not None}

                item = hydrate(  # type: ignore
                    base_item,
                    dict(item),
                    strip_unmatched_markers=settings.exclude_hydrate_markers,
                )

                # Grab ids needed for links that may be removed by the fields extension.
                collection_id = item.get("collection")
                item_id = item.get("id")

                item = filter_fields(item, include, exclude)
                await _add_item_links(item, collection_id, item_id)
                items.append(item)

        else:
            for item in item_collection.get("features", []):
                await _add_item_links(item)
                items.append(item)

        item_collection["features"] = items
        item_collection["links"] = await PagingLinks(
            request=request,
            next=next,
            prev=prev,
        ).get_links()

        return item_collection

    async def item_collection(  # type: ignore [override]
        self,
        collection_id: str,
        request: Request,
        bbox: BBox | None = None,
        datetime: str | None = None,
        limit: int | None = None,
        # Extensions
        query: str | None = None,
        fields: list[str] | None = None,
        sortby: str | None = None,
        filter_expr: str | None = None,
        filter_lang: str | None = None,
        token: str | None = None,
        **kwargs: Any,
    ) -> ItemCollection:
        """Get all items from a specific collection.

        Called with `GET /collections/{collection_id}/items`

        Args:
            collection_id: id of the collection.
            limit: number of items to return.
            token: pagination token.

        Returns:
            An ItemCollection.
        """
        # If collection does not exist, NotFoundError wil be raised
        await self.get_collection(collection_id, request=request)

        base_args = {
            "collections": [collection_id],
            "bbox": bbox,
            "datetime": datetime,
            "limit": limit,
            "token": token,
            "query": orjson.loads(unquote_plus(query)) if query else query,
        }

        clean = self._clean_search_args(
            base_args=base_args,
            filter_query=filter_expr,
            filter_lang=filter_lang,
            fields=fields,
            sortby=sortby,
            **kwargs,
        )

        try:
            search_request = self.pgstac_search_model(**clean)
        except ValidationError as e:
            raise HTTPException(
                status_code=400, detail=f"Invalid parameters provided {e}"
            ) from e

        item_collection = await self._search_base(search_request, request=request)

        links = await ItemCollectionLinks(
            collection_id=collection_id, request=request
        ).get_links(extra_links=item_collection["links"])
        item_collection["links"] = links

        # If we have the `fields` extension enabled
        # we need to avoid Pydantic validation because the
        # Items might not be a valid STAC Item objects
        if fields := getattr(search_request, "fields", None):
            if fields.include or fields.exclude:
                return JSONResponse(item_collection)  # type: ignore

        return ItemCollection(**item_collection)

    async def get_item(  # type: ignore [override]
        self,
        item_id: str,
        collection_id: str,
        request: Request,
        **kwargs: Any,
    ) -> Item:
        """Get item by id.

        Called with `GET /collections/{collection_id}/items/{item_id}`.

        Args:
            item_id: ID of the item.
            collection_id: ID of the collection the item is in.

        Returns:
            Item.
        """
        # If collection does not exist, NotFoundError wil be raised
        await self.get_collection(collection_id, request=request)

        search_request = self.pgstac_search_model(
            ids=[item_id], collections=[collection_id], limit=1
        )
        item_collection = await self._search_base(search_request, request=request)
        if not item_collection["features"]:
            raise NotFoundError(
                f"Item {item_id} in Collection {collection_id} does not exist."
            )

        return item_collection["features"][0]

    async def post_search(  # type: ignore [override]
        self,
        search_request: PgstacSearch,
        request: Request,
        **kwargs: Any,
    ) -> ItemCollection:
        """Cross catalog search (POST).

        Called with `POST /search`.

        Args:
            search_request: search request parameters.

        Returns:
            ItemCollection containing items which match the search criteria.
        """
        item_collection = await self._search_base(search_request, request=request)

        # If we have the `fields` extension enabled
        # we need to avoid Pydantic validation because the
        # Items might not be a valid STAC Item objects
        if fields := getattr(search_request, "fields", None):
            if fields.include or fields.exclude:
                return JSONResponse(item_collection)  # type: ignore

        links = await SearchLinks(request=request).get_links(
            extra_links=item_collection["links"]
        )
        item_collection["links"] = links

        return item_collection

    async def get_search(  # type: ignore [override]
        self,
        request: Request,
        collections: list[str] | None = None,
        ids: list[str] | None = None,
        bbox: BBox | None = None,
        intersects: str | None = None,
        datetime: str | None = None,
        limit: int | None = None,
        # Extensions
        query: str | None = None,
        fields: list[str] | None = None,
        sortby: str | None = None,
        filter_expr: str | None = None,
        filter_lang: str | None = None,
        token: str | None = None,
        **kwargs: Any,
    ) -> ItemCollection:
        """Cross catalog search (GET).

        Called with `GET /search`.

        Returns:
            ItemCollection containing items which match the search criteria.
        """
        # Parse request parameters
        base_args = {
            "collections": collections,
            "ids": ids,
            "bbox": bbox,
            "limit": limit,
            "token": token,
            "query": orjson.loads(unquote_plus(query)) if query else query,
        }

        clean = self._clean_search_args(
            base_args=base_args,
            intersects=intersects,
            datetime=datetime,
            fields=fields,
            sortby=sortby,
            filter_query=filter_expr,
            filter_lang=filter_lang,
            **kwargs,
        )

        try:
            search_request = self.pgstac_search_model(**clean)
        except ValidationError as e:
            raise HTTPException(
                status_code=400, detail=f"Invalid parameters provided {e}"
            ) from e

        item_collection = await self._search_base(search_request, request=request)

        links = await SearchLinks(request=request).get_links(
            extra_links=item_collection["links"]
        )
        item_collection["links"] = links

        # If we have the `fields` extension enabled
        # we need to avoid Pydantic validation because the
        # Items might not be a valid STAC Item objects
        if fields := getattr(search_request, "fields", None):
            if fields.include or fields.exclude:
                return JSONResponse(item_collection)  # type: ignore

        return item_collection

    def _clean_search_args(  # noqa: C901
        self,
        base_args: dict[str, Any],
        intersects: str | None = None,
        datetime: str | None = None,
        fields: list[str] | None = None,
        sortby: str | None = None,
        filter_query: str | None = None,
        filter_lang: str | None = None,
        q: str | list[str] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Clean up search arguments to match format expected by pgstac"""
        if filter_query:
            if filter_lang == "cql2-text":
                e = Expr(filter_query)
                base_args["filter"] = e.to_json()
                base_args["filter_lang"] = "cql2-json"
            else:
                base_args["filter"] = orjson.loads(filter_query)
                base_args["filter_lang"] = filter_lang

        if datetime:
            base_args["datetime"] = datetime

        if intersects:
            base_args["intersects"] = orjson.loads(unquote_plus(intersects))

        if sortby:
            # https://github.com/radiantearth/stac-spec/tree/master/api-spec/extensions/sort#http-get-or-post-form
            sort_param = []
            for sort in sortby:
                sortparts = re.match(r"^([+-]?)(.*)$", sort)
                if sortparts:
                    sort_param.append(
                        {
                            "field": sortparts.group(2).strip(),
                            "direction": "desc" if sortparts.group(1) == "-" else "asc",
                        }
                    )
            base_args["sortby"] = sort_param

        if fields:
            includes = set()
            excludes = set()
            for field in fields:
                if field[0] == "-":
                    excludes.add(field[1:])
                elif field[0] == "+":
                    includes.add(field[1:])
                else:
                    includes.add(field)

            base_args["fields"] = {"include": list(includes), "exclude": list(excludes)}

        if q:
            base_args["q"] = q

        # Remove None values from dict
        clean = {}
        for k, v in base_args.items():
            if v is not None and v != []:
                clean[k] = v

        return clean


async def health_check(request: Request) -> dict | JSONResponse:
    """PgSTAC HealthCheck."""
    resp = {
        "status": "UP",
        "lifespan": {
            "status": "UP",
        },
    }
    if not hasattr(request.app.state, "get_connection"):
        return JSONResponse(
            status_code=503,
            content={
                "status": "DOWN",
                "lifespan": {
                    "status": "DOWN",
                    "message": "application lifespan wasn't run",
                },
                "pgstac": {
                    "status": "DOWN",
                    "message": "Could not connect to database",
                },
            },
        )

    try:
        async with request.app.state.get_connection(request, "r") as conn:
            q, p = render(
                """SELECT pgstac.get_version();""",
            )
            version = await conn.fetchval(q, *p)
    except Exception as e:
        resp["status"] = "DOWN"
        resp["pgstac"] = {
            "status": "DOWN",
            "message": str(e),
        }
        return JSONResponse(status_code=503, content=resp)

    resp["pgstac"] = {
        "status": "UP",
        "pgstac_version": version,
    }

    return resp
