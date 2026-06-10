from stac_fastapi.extensions.core import CollectionSearchExtension
from stac_fastapi.types.extension import ApiExtension

from stac_fastapi.pgstac.config import Settings
from stac_fastapi.pgstac.models.extensions import DEFAULT_EXTENSIONS, Extensions


class TestApiExtension(ApiExtension):
    def register(self, app) -> None:
        pass


class CustomQueryExtension(TestApiExtension):
    pass


class CustomNewExtension(TestApiExtension):
    pass


def test_extensions_default():
    extensions = Extensions()
    assert extensions.search == list(DEFAULT_EXTENSIONS["search_map"].values())
    assert (
        extensions.collection_search.conformance_classes
        == CollectionSearchExtension.from_extensions(
            list(DEFAULT_EXTENSIONS["collection_search_map"].values())
        ).conformance_classes
    )
    assert extensions.item_collection == list(
        DEFAULT_EXTENSIONS["item_collection_map"].values()
    )
    assert extensions.transaction == []
    assert extensions.extra == []


def test_extensions_enabled():
    settings = Settings(enabled_extensions=["query", "sort", "collection_search"])
    extensions = Extensions(settings=settings)
    assert extensions.search == [
        DEFAULT_EXTENSIONS["search_map"]["query"],
        DEFAULT_EXTENSIONS["search_map"]["sort"],
    ]
    assert (
        extensions.collection_search.conformance_classes
        == CollectionSearchExtension.from_extensions(
            [
                DEFAULT_EXTENSIONS["collection_search_map"]["query"],
                DEFAULT_EXTENSIONS["collection_search_map"]["sort"],
            ]
        ).conformance_classes
    )
    assert extensions.item_collection == [
        DEFAULT_EXTENSIONS["item_collection_map"]["query"],
        DEFAULT_EXTENSIONS["item_collection_map"]["sort"],
    ]
    assert extensions.transaction == []
    assert extensions.extra == []


def test_extensions_enabled_no_collection_search():
    settings = Settings(enabled_extensions=["query", "sort"])
    extensions = Extensions(settings=settings)
    assert extensions.search == [
        DEFAULT_EXTENSIONS["search_map"]["query"],
        DEFAULT_EXTENSIONS["search_map"]["sort"],
    ]
    assert extensions.collection_search is None
    assert extensions.item_collection == [
        DEFAULT_EXTENSIONS["item_collection_map"]["query"],
        DEFAULT_EXTENSIONS["item_collection_map"]["sort"],
    ]
    assert extensions.transaction == []
    assert extensions.extra == []


def test_extensions_enabled_transactions():
    settings = Settings(enable_transactions_extensions=True)
    extensions = Extensions(settings=settings)
    assert len(extensions.transaction) == 2


def test_extensions_custom():
    custom_query_extension = CustomQueryExtension()
    custom_new_extension = CustomNewExtension()
    extensions = Extensions(
        search_map={"query": custom_query_extension},
        extra_map={"new": custom_new_extension},
    )
    assert extensions.search == list(
        {
            **DEFAULT_EXTENSIONS["search_map"],
            **{"query": custom_query_extension},
        }.values()
    )
    assert (
        extensions.collection_search.conformance_classes
        == CollectionSearchExtension.from_extensions(
            list(DEFAULT_EXTENSIONS["collection_search_map"].values())
        ).conformance_classes
    )
    assert extensions.item_collection == list(
        DEFAULT_EXTENSIONS["item_collection_map"].values()
    )
    assert extensions.transaction == []
    assert extensions.extra == [custom_new_extension]
