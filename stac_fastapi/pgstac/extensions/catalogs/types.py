"""Request types for the catalogs extension."""

from typing import Annotated

import attr
from fastapi import Query
from stac_fastapi.types.search import (
    DateTimeQueryType,
    _bbox_converter,
    _validate_datetime,
)
from stac_fastapi_catalogs_extension.types import (
    CatalogCollectionItemsRequest,
    CatalogCollectionsRequest,
)
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
    datetime: DateTimeQueryType = attr.ib(default=None, validator=_validate_datetime)


@attr.s
class PgstacCatalogCollectionItemsRequest(CatalogCollectionItemsRequest):
    """Request model for `GET /catalogs/{catalog_id}/collections/{collection_id}/items`.

    Re-declares the library's `datetime` parameter with the same RFC 3339
    validator `GET /collections/{collection_id}/items` uses, so invalid values
    are rejected with a 400 instead of reaching pgstac and erroring with a 500.
    """

    datetime: DateTimeQueryType = attr.ib(default=None, validator=_validate_datetime)
