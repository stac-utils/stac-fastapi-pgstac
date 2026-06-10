from dataclasses import dataclass, field

from stac_fastapi.api.models import JSONResponse
from stac_fastapi.extensions.core import (
    CollectionSearchExtension,
    CollectionSearchFilterExtension,
    FieldsExtension,
    ItemCollectionFilterExtension,
    OffsetPaginationExtension,
    SearchFilterExtension,
    SortExtension,
    TokenPaginationExtension,
    TransactionExtension,
)
from stac_fastapi.extensions.core.fields import FieldsConformanceClasses
from stac_fastapi.extensions.core.free_text import FreeTextConformanceClasses
from stac_fastapi.extensions.core.query import QueryConformanceClasses
from stac_fastapi.extensions.core.sort import SortConformanceClasses
from stac_fastapi.extensions.third_party import BulkTransactionExtension
from stac_fastapi.types.extension import ApiExtension

from stac_fastapi.pgstac.config import Settings
from stac_fastapi.pgstac.extensions import FreeTextExtension, QueryExtension
from stac_fastapi.pgstac.extensions.filter import FiltersClient
from stac_fastapi.pgstac.transactions import BulkTransactionsClient, TransactionsClient

DEFAULT_EXTENSIONS = {
    "search_map": {
        "query": QueryExtension(),
        "sort": SortExtension(),
        "fields": FieldsExtension(),
        "filter": SearchFilterExtension(client=FiltersClient()),
        "pagination": TokenPaginationExtension(),
    },
    "collection_search_map": {
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
    "item_collection_map": {
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


@dataclass
class Extensions:
    """Updating the default extensions. Provided extensions are merged with defaults."""

    search_map: dict[str, ApiExtension] = field(default_factory=dict)
    collection_search_map: dict[str, ApiExtension] = field(default_factory=dict)
    item_collection_map: dict[str, ApiExtension] = field(default_factory=dict)
    extra_map: dict[str, ApiExtension] = field(default_factory=dict)
    settings: Settings = field(default_factory=Settings)

    def get_enabled_extensions(self, key: str) -> list[ApiExtension]:
        extensions_map_with_defaults = {
            **DEFAULT_EXTENSIONS.get(f"{key}_map", {}),
            **getattr(self, f"{key}_map", {}),
        }
        enabled_extensions_keys = self.settings.enabled_extensions
        if enabled_extensions_keys is None:
            enabled_extensions = list(extensions_map_with_defaults.values())
        else:
            enabled_extensions = [
                extension
                for k, extension in extensions_map_with_defaults.items()
                if k in enabled_extensions_keys
            ]
        return enabled_extensions

    @property
    def search(self) -> list[ApiExtension]:
        return self.get_enabled_extensions("search")

    @property
    def item_collection(self) -> list[ApiExtension]:
        return self.get_enabled_extensions("item_collection")

    @property
    def collection_search(self) -> CollectionSearchExtension | None:
        if (
            self.settings.enabled_extensions is None
            or "collection_search" in self.settings.enabled_extensions
        ):
            extensions_enabled = self.get_enabled_extensions("collection_search")
            return CollectionSearchExtension.from_extensions(extensions_enabled)
        return None

    @property
    def transaction(self) -> list[ApiExtension]:
        extensions_enabled: list[ApiExtension] = []
        if self.settings.enable_transactions_extensions:
            extensions_enabled.append(
                TransactionExtension(
                    client=TransactionsClient(),
                    settings=self.settings,
                    response_class=JSONResponse,
                ),
            )
            extensions_enabled.append(
                BulkTransactionExtension(client=BulkTransactionsClient()),
            )
        return extensions_enabled

    @property
    def extra(self) -> list[ApiExtension]:
        return self.get_enabled_extensions("extra")
