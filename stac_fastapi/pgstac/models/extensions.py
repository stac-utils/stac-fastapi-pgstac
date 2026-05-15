from dataclasses import dataclass, field

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

DEFAULT_EXTENSIONS = {
    "search": {
        "query": QueryExtension(),
        "sort": SortExtension(),
        "fields": FieldsExtension(),
        "filter": SearchFilterExtension(client=FiltersClient()),
        "pagination": TokenPaginationExtension(),
    },
    "collection_search": {
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
    },
    "item_collection": {
        "query": QueryExtension(
            conformance_classes=[QueryConformanceClasses.ITEMS],
        ),
        "sort": SortExtension(
            conformance_classes=[SortConformanceClasses.ITEMS],
        ),
        "fields": FieldsExtension(conformance_classes=[FieldsConformanceClasses.ITEMS]),
        "filter": ItemCollectionFilterExtension(client=FiltersClient()),
        "pagination": TokenPaginationExtension(),
    },
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


@dataclass
class Extensions:
    """Updating the default extensions. Provided extensions are merged with defaults."""

    search: dict[str, ApiExtension] = field(default_factory=dict)
    collection_search: dict[str, ApiExtension] = field(default_factory=dict)
    item_collection: dict[str, ApiExtension] = field(default_factory=dict)
    extra: dict[str, ApiExtension] = field(default_factory=dict)

    def __post_init__(self):
        for field_name in ("search", "collection_search", "item_collection", "extra"):
            value = getattr(self, field_name)
            invalid = {
                k: type(v).__name__
                for k, v in value.items()
                if not isinstance(v, ApiExtension)
            }
            if invalid:
                raise TypeError(
                    f"'{field_name}' contains values that are not ApiExtension instances: {invalid}"
                )
            default = DEFAULT_EXTENSIONS.get(field_name)
            setattr(
                self, field_name, get_stac_api_extensions(default=default, update=value)
            )

    @property
    def default_enabled(self) -> set[str]:
        """Return the unique keys from all extension groups plus 'collection_search'."""
        return {
            *self.search.keys(),
            *self.collection_search.keys(),
            *self.item_collection.keys(),
            *self.extra.keys(),
            "collection_search",
        }
