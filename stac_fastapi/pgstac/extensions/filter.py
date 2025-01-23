"""Get Queryables."""

from typing import Any, Dict, List, Optional, Type

import attr
from buildpg import render
from fastapi import APIRouter, FastAPI, Request
from stac_fastapi.api.models import CollectionUri, EmptyRequest, JSONSchemaResponse
from stac_fastapi.api.routes import create_async_endpoint
from stac_fastapi.extensions.core import FilterExtension
from stac_fastapi.extensions.core.collection_search.collection_search import (
    ConformanceClasses as CollectionSearchConformanceClasses,
)
from stac_fastapi.extensions.core.filter.client import AsyncBaseFiltersClient
from stac_fastapi.extensions.core.filter.filter import FilterConformanceClasses
from stac_fastapi.extensions.core.filter.request import (
    FilterExtensionGetRequest,
    FilterExtensionPostRequest,
)
from stac_fastapi.types.errors import NotFoundError
from starlette.responses import Response


class FiltersClient(AsyncBaseFiltersClient):
    """Defines a pattern for implementing the STAC filter extension."""

    async def get_queryables(
        self,
        request: Request,
        collection_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Get the queryables available for the given collection_id.

        If collection_id is None, returns the intersection of all
        queryables over all collections.
        This base implementation returns a blank queryable schema. This is not allowed
        under OGC CQL but it is allowed by the STAC API Filter Extension
        https://github.com/radiantearth/stac-api-spec/tree/master/fragments/filter#queryables
        """
        pool = request.app.state.readpool

        async with pool.acquire() as conn:
            q, p = render(
                """
                    SELECT * FROM get_queryables(:collection::text);
                """,
                collection=collection_id,
            )
            queryables = await conn.fetchval(q, *p)
            if not queryables:
                raise NotFoundError(f"Collection {collection_id} not found")

            queryables["$id"] = str(request.url)
            return queryables


@attr.s
class SearchFilterExtension(FilterExtension):
    """Item Search Filter Extension."""

    GET = FilterExtensionGetRequest
    POST = FilterExtensionPostRequest

    client: FiltersClient = attr.ib(factory=FiltersClient)
    conformance_classes: List[str] = attr.ib(
        default=[
            FilterConformanceClasses.FILTER,
            FilterConformanceClasses.ITEM_SEARCH_FILTER,
            FilterConformanceClasses.BASIC_CQL2,
            FilterConformanceClasses.CQL2_JSON,
            FilterConformanceClasses.CQL2_TEXT,
        ]
    )
    router: APIRouter = attr.ib(factory=APIRouter)
    response_class: Type[Response] = attr.ib(default=JSONSchemaResponse)

    def register(self, app: FastAPI) -> None:
        """Register the extension with a FastAPI application.

        Args:
            app: target FastAPI application.

        Returns:
            None
        """
        self.router.prefix = app.state.router_prefix
        self.router.add_api_route(
            name="Queryables",
            path="/queryables",
            methods=["GET"],
            responses={
                200: {
                    "content": {
                        "application/schema+json": {},
                    },
                    # TODO: add output model in stac-pydantic
                },
            },
            response_class=self.response_class,
            endpoint=create_async_endpoint(self.client.get_queryables, EmptyRequest),
        )
        app.include_router(self.router, tags=["Filter Extension"])


@attr.s
class ItemCollectionFilterExtension(FilterExtension):
    """Item Collection Filter Extension."""

    GET = FilterExtensionGetRequest
    POST = FilterExtensionPostRequest

    client: FiltersClient = attr.ib(factory=FiltersClient)
    conformance_classes: List[str] = attr.ib(
        default=[
            FilterConformanceClasses.FILTER,
            FilterConformanceClasses.FEATURES_FILTER,
            FilterConformanceClasses.BASIC_CQL2,
            FilterConformanceClasses.CQL2_JSON,
            FilterConformanceClasses.CQL2_TEXT,
        ]
    )
    router: APIRouter = attr.ib(factory=APIRouter)
    response_class: Type[Response] = attr.ib(default=JSONSchemaResponse)

    def register(self, app: FastAPI) -> None:
        """Register the extension with a FastAPI application.

        Args:
            app: target FastAPI application.

        Returns:
            None
        """
        self.router.add_api_route(
            name="Collection Queryables",
            path="/collections/{collection_id}/queryables",
            methods=["GET"],
            responses={
                200: {
                    "content": {
                        "application/schema+json": {},
                    },
                    # TODO: add output model in stac-pydantic
                },
            },
            response_class=self.response_class,
            endpoint=create_async_endpoint(self.client.get_queryables, CollectionUri),
        )
        app.include_router(self.router, tags=["Filter Extension"])


@attr.s
class CollectionSearchFilterExtension(FilterExtension):
    """Collection Search Filter Extension."""

    GET = FilterExtensionGetRequest
    POST = FilterExtensionPostRequest

    client: FiltersClient = attr.ib(factory=FiltersClient)
    conformance_classes: List[str] = attr.ib(
        default=[CollectionSearchConformanceClasses.FILTER]
    )
    router: APIRouter = attr.ib(factory=APIRouter)
    response_class: Type[Response] = attr.ib(default=JSONSchemaResponse)

    def register(self, app: FastAPI) -> None:
        """Register the extension with a FastAPI application.

        Args:
            app: target FastAPI application.

        Returns:
            None
        """
        pass
