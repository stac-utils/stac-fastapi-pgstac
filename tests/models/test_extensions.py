from contextlib import asynccontextmanager

import pytest
from httpx import ASGITransport, AsyncClient
from stac_fastapi.api.app import StacApi
from stac_fastapi.extensions import CollectionSearchExtension
from stac_fastapi.types.extension import ApiExtension

from stac_fastapi.pgstac.app import instantiate_api
from stac_fastapi.pgstac.config import Settings
from stac_fastapi.pgstac.core import CoreCrudClient
from stac_fastapi.pgstac.models.extensions import Extensions, get_default_extensions_map


@asynccontextmanager
async def noop_lifespan(app):
    """Lifespan that skips connecting to the database."""
    yield


class TestApiExtension(ApiExtension):
    def register(self, app) -> None:
        pass


class CustomQueryExtension(TestApiExtension):
    pass


class CustomNewExtension(TestApiExtension):
    pass


def test_extensions_default():
    extensions = Extensions()
    assert extensions.search == list(get_default_extensions_map("search_map").values())
    assert (
        extensions.collection_search.conformance_classes
        == CollectionSearchExtension.from_extensions(
            list(get_default_extensions_map("collection_search_map").values())
        ).conformance_classes
    )
    assert extensions.item_collection == list(
        get_default_extensions_map("item_collection_map").values()
    )
    assert extensions.transaction == []
    assert extensions.extra == []


@pytest.mark.parametrize(
    "key,filter_conformance",
    [
        (
            "search_map",
            "https://api.stacspec.org/v1.0.0-rc.2/item-search#filter",
        ),
        (
            "item_collection_map",
            "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/features-filter",
        ),
        (
            "collection_search_map",
            "https://api.stacspec.org/v1.0.0-rc.1/collection-search#filter",
        ),
    ],
)
async def test_default_filter_conformance(key, filter_conformance):
    api = StacApi(
        settings=Settings(testing=True),
        client=CoreCrudClient(),
        extensions=[get_default_extensions_map(key)["filter"]],
    )
    async with AsyncClient(
        transport=ASGITransport(app=api.app), base_url="http://test"
    ) as client:
        response = await client.get("/conformance")

    assert response.status_code == 200
    conformance = response.json()["conformsTo"]
    assert filter_conformance in conformance
    assert "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/filter" in conformance
    assert {
        f"http://www.opengis.net/spec/cql2/1.0/conf/{name}"
        for name in (
            "basic-cql2",
            "cql2-json",
            "cql2-text",
            "advanced-comparison-operators",
            "case-insensitive-comparison",
            "accent-insensitive-comparison",
            "property-property",
        )
    }.issubset(conformance)


def test_extensions_enabled():
    settings = Settings(enabled_extensions=["query", "sort", "collection_search"])
    extensions = Extensions(settings=settings)
    assert extensions.search == [
        get_default_extensions_map("search_map")["query"],
        get_default_extensions_map("search_map")["sort"],
    ]
    assert (
        extensions.collection_search.conformance_classes
        == CollectionSearchExtension.from_extensions(
            [
                get_default_extensions_map("collection_search_map")["query"],
                get_default_extensions_map("collection_search_map")["sort"],
            ]
        ).conformance_classes
    )
    assert extensions.item_collection == [
        get_default_extensions_map("item_collection_map")["query"],
        get_default_extensions_map("item_collection_map")["sort"],
    ]
    assert extensions.transaction == []
    assert extensions.extra == []


def test_extensions_enabled_no_collection_search():
    settings = Settings(enabled_extensions=["query", "sort"])
    extensions = Extensions(settings=settings)
    assert extensions.search == [
        get_default_extensions_map("search_map")["query"],
        get_default_extensions_map("search_map")["sort"],
    ]
    assert extensions.collection_search is None
    assert extensions.item_collection == [
        get_default_extensions_map("item_collection_map")["query"],
        get_default_extensions_map("item_collection_map")["sort"],
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
            **get_default_extensions_map("search_map"),
            **{"query": custom_query_extension},
        }.values()
    )
    assert (
        extensions.collection_search.conformance_classes
        == CollectionSearchExtension.from_extensions(
            list(get_default_extensions_map("collection_search_map").values())
        ).conformance_classes
    )
    assert extensions.item_collection == list(
        get_default_extensions_map("item_collection_map").values()
    )
    assert extensions.transaction == []
    assert extensions.extra == [custom_new_extension]


def test_extensions_modify_default():
    extensions_a = Extensions()
    extensions_b = Extensions()
    extensions_a.search_map["query"].conformance_classes.append("custom-sort-feature")

    assert "custom-sort-feature" in extensions_a.search_map["query"].conformance_classes
    assert (
        "custom-sort-feature" not in extensions_b.search_map["query"].conformance_classes
    )


@pytest.mark.parametrize(
    "enabled_extensions,expected",
    [
        (None, {"fields", "sortby", "q", "filter", "filter-lang", "filter-crs"}),
        (["sort", "free_text"], {"sortby", "q"}),
    ],
)
def test_catalog_collections_search_params_in_openapi(enabled_extensions, expected):
    settings = Settings(
        testing=True,
        enable_catalogs_extension=True,
        enabled_extensions=enabled_extensions,
    )
    api = instantiate_api(
        extensions=Extensions(settings=settings),
        settings=settings,
        lifespan=noop_lifespan,
    )

    operation = api.app.openapi()["paths"]["/catalogs/{catalog_id}/collections"]["get"]
    params = {p["name"] for p in operation["parameters"]}

    assert {"catalog_id", "limit", "token"} <= params
    search_params = {"fields", "sortby", "q", "filter", "filter-lang", "filter-crs"}
    assert params & search_params == expected
    assert "query" not in params
    assert "offset" not in params
