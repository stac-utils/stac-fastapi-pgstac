"""Request types for the catalogs extension."""

from typing import Annotated

import attr
from fastapi import Query
from stac_fastapi.types.search import _bbox_converter
from stac_fastapi_catalogs_extension.types import CatalogCollectionsRequest
from stac_pydantic.shared import BBox


@attr.s
class PgstacCatalogCollectionsRequest(CatalogCollectionsRequest):
    """Request model for `GET /catalogs/{catalog_id}/collections`.

    Adds the Collection Search `bbox` and `datetime` parameters on top of the
    library's pagination primitives so the scoped listing can claim the
    `multi-tenant-catalogs#collection-search` marker, which requires the
    Collection Search Basics.
    """

    bbox: Annotated[
        BBox | None,
        Query(description="Bounding box to filter collections [minx, miny, maxx, maxy]"),
    ] = attr.ib(
        default=None,
        converter=lambda x: _bbox_converter(x) if x is not None else None,
    )
    datetime: Annotated[
        str | None, Query(description="Datetime to filter collections")
    ] = attr.ib(default=None)
