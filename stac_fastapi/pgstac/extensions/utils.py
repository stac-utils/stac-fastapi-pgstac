from stac_fastapi.extensions.core import (
    CollectionSearchFilterExtension,
    FieldsExtension,
    ItemCollectionFilterExtension,
    OffsetPaginationExtension,
    SearchFilterExtension,
    SortExtension,
    TokenPaginationExtension,
)
from stac_fastapi.extensions.core.fields import FieldsConformanceClasses
from stac_fastapi.extensions.core.free_text import FreeTextConformanceClasses
from stac_fastapi.extensions.core.query import QueryConformanceClasses
from stac_fastapi.extensions.core.sort import SortConformanceClasses
from stac_fastapi.types.extension import ApiExtension

from stac_fastapi.pgstac.extensions import FreeTextExtension, QueryExtension
from stac_fastapi.pgstac.extensions.filter import FiltersClient


def get_default_search_extensions() -> dict[str, ApiExtension]:
    """Get the default search extensions."""
    return {
        "query": QueryExtension(),
        "sort": SortExtension(),
        "fields": FieldsExtension(),
        "filter": SearchFilterExtension(client=FiltersClient()),
        "pagination": TokenPaginationExtension(),
    }


def get_default_collection_search_extensions() -> dict[str, ApiExtension]:
    """Get the default collection search extensions."""
    return {
        "query": QueryExtension(
            conformance_classes=[QueryConformanceClasses.COLLECTIONS]
        ),
        "sort": SortExtension(conformance_classes=[SortConformanceClasses.COLLECTIONS]),
        "fields": FieldsExtension(
            conformance_classes=[FieldsConformanceClasses.COLLECTIONS]
        ),
        "filter": CollectionSearchFilterExtension(client=FiltersClient()),
        "free_text": FreeTextExtension(
            conformance_classes=[FreeTextConformanceClasses.COLLECTIONS],
        ),
        "pagination": OffsetPaginationExtension(),
    }


def get_default_item_collection_extensions() -> dict[str, ApiExtension]:
    """Get the default item collection extensions."""
    return {
        "query": QueryExtension(
            conformance_classes=[QueryConformanceClasses.ITEMS],
        ),
        "sort": SortExtension(
            conformance_classes=[SortConformanceClasses.ITEMS],
        ),
        "fields": FieldsExtension(conformance_classes=[FieldsConformanceClasses.ITEMS]),
        "filter": ItemCollectionFilterExtension(client=FiltersClient()),
        "pagination": TokenPaginationExtension(),
    }


def get_stac_api_extensions(
    update: dict[str, ApiExtension] | None = None,
    default: dict[str, ApiExtension] | None = None,
) -> dict[str, ApiExtension]:
    """Get the STAC API extensions."""
    extensions = default or {}
    if update:
        extensions.update(update)
    return extensions
