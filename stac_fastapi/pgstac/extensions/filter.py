"""Get Queryables."""

from typing import Any

from buildpg import render
from fastapi import Request
from stac_fastapi.extensions.core.filter.client import AsyncBaseFiltersClient
from stac_fastapi.types.errors import NotFoundError


class FiltersClient(AsyncBaseFiltersClient):
    """Defines a pattern for implementing the STAC filter extension."""

    async def get_queryables(  # type: ignore[override]
        self,
        request: Request,
        collection_id: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Get the queryables available for the given collection_id.

        If collection_id is None, returns the intersection of all
        queryables over all collections.
        This base implementation returns a blank queryable schema. This is not allowed
        under OGC CQL but it is allowed by the STAC API Filter Extension
        https://github.com/radiantearth/stac-api-spec/tree/master/fragments/filter#queryables
        """
        async with request.app.state.get_connection(request, "r") as conn:
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
