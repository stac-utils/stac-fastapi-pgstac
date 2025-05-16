import json
import logging
import os
import time
from typing import Callable, Dict
from urllib.parse import quote_plus as quote
from urllib.parse import urljoin

import asyncpg
import pytest
from fastapi import APIRouter
from httpx import ASGITransport, AsyncClient
from pypgstac import __version__ as pgstac_version
from pypgstac.db import PgstacDB
from pypgstac.migrate import Migrate
from pytest_postgresql.janitor import DatabaseJanitor
from stac_fastapi.api.app import StacApi
from stac_fastapi.api.models import (
    ItemCollectionUri,
    JSONResponse,
    create_get_request_model,
    create_post_request_model,
    create_request_model,
)
from stac_fastapi.extensions.core import (
    CollectionSearchExtension,
    CollectionSearchFilterExtension,
    FieldsExtension,
    FreeTextExtension,
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
from stac_pydantic import Collection, Item

from stac_fastapi.pgstac.config import PostgresSettings, Settings
from stac_fastapi.pgstac.core import CoreCrudClient, health_check
from stac_fastapi.pgstac.db import close_db_connection, connect_to_db
from stac_fastapi.pgstac.extensions import QueryExtension
from stac_fastapi.pgstac.extensions.filter import FiltersClient
from stac_fastapi.pgstac.transactions import BulkTransactionsClient, TransactionsClient
from stac_fastapi.pgstac.types.search import PgstacSearch

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


logger = logging.getLogger(__name__)


requires_pgstac_0_9_2 = pytest.mark.skipif(
    tuple(map(int, pgstac_version.split("."))) < (0, 9, 2),
    reason="PgSTAC>=0.9.2 required",
)


@pytest.fixture(scope="session")
def database(postgresql_proc):
    with DatabaseJanitor(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        dbname="pgstactestdb",
        version=postgresql_proc.version,
        password="a2Vw:yk=)CdSis[fek]tW=/o",
    ) as jan:
        connection = f"postgresql://{jan.user}:{quote(jan.password)}@{jan.host}:{jan.port}/{jan.dbname}"
        with PgstacDB(dsn=connection) as db:
            migrator = Migrate(db)
            version = migrator.run_migration()
            assert version

        yield jan


@pytest.fixture(autouse=True)
async def pgstac(database):
    connection = f"postgresql://{database.user}:{quote(database.password)}@{database.host}:{database.port}/{database.dbname}"
    yield
    conn = await asyncpg.connect(dsn=connection)
    await conn.execute(
        """
        DROP SCHEMA IF EXISTS pgstac CASCADE;
        """
    )
    await conn.close()
    with PgstacDB(dsn=connection) as db:
        migrator = Migrate(db)
        version = migrator.run_migration()

    logger.info(f"PGStac Migrated to {version}")


# Run all the tests that use the api_client in both db hydrate and api hydrate mode
@pytest.fixture(
    params=[
        # hydratation, prefix, model_validation
        (False, "", False),
        (False, "/router_prefix", False),
        (True, "", False),
        (True, "/router_prefix", False),
        (False, "", True),
        (True, "", True),
    ],
    scope="session",
)
def api_client(request):
    hydrate, prefix, response_model = request.param
    api_settings = Settings(
        enable_response_models=response_model,
        testing=True,
        use_api_hydrate=hydrate,
    )

    api_settings.openapi_url = prefix + api_settings.openapi_url
    api_settings.docs_url = prefix + api_settings.docs_url

    logger.info(
        "creating client with settings, hydrate: {}, router prefix: '{}'".format(
            api_settings.use_api_hydrate, prefix
        )
    )

    application_extensions = [
        TransactionExtension(client=TransactionsClient(), settings=api_settings),
        BulkTransactionExtension(client=BulkTransactionsClient()),
    ]

    search_extensions = [
        QueryExtension(),
        SortExtension(),
        FieldsExtension(),
        SearchFilterExtension(client=FiltersClient()),
        TokenPaginationExtension(),
    ]
    application_extensions.extend(search_extensions)

    collection_extensions = [
        QueryExtension(conformance_classes=[QueryConformanceClasses.COLLECTIONS]),
        SortExtension(conformance_classes=[SortConformanceClasses.COLLECTIONS]),
        FieldsExtension(conformance_classes=[FieldsConformanceClasses.COLLECTIONS]),
        CollectionSearchFilterExtension(client=FiltersClient()),
        FreeTextExtension(
            conformance_classes=[FreeTextConformanceClasses.COLLECTIONS],
        ),
        OffsetPaginationExtension(),
    ]
    collection_search_extension = CollectionSearchExtension.from_extensions(
        collection_extensions
    )
    application_extensions.append(collection_search_extension)

    item_collection_extensions = [
        QueryExtension(
            conformance_classes=[QueryConformanceClasses.ITEMS],
        ),
        SortExtension(
            conformance_classes=[SortConformanceClasses.ITEMS],
        ),
        FieldsExtension(conformance_classes=[FieldsConformanceClasses.ITEMS]),
        ItemCollectionFilterExtension(client=FiltersClient()),
        TokenPaginationExtension(),
    ]
    application_extensions.extend(item_collection_extensions)

    items_get_request_model = create_request_model(
        model_name="ItemCollectionUri",
        base_model=ItemCollectionUri,
        extensions=item_collection_extensions,
        request_type="GET",
    )
    search_get_request_model = create_get_request_model(search_extensions)
    search_post_request_model = create_post_request_model(
        search_extensions, base_model=PgstacSearch
    )

    api = StacApi(
        settings=api_settings,
        extensions=application_extensions,
        client=CoreCrudClient(pgstac_search_model=search_post_request_model),
        items_get_request_model=items_get_request_model,
        search_get_request_model=search_get_request_model,
        search_post_request_model=search_post_request_model,
        collections_get_request_model=collection_search_extension.GET,
        response_class=JSONResponse,
        router=APIRouter(prefix=prefix),
        health_check=health_check,
    )

    return api


@pytest.fixture(scope="function")
async def app(api_client, database):
    postgres_settings = PostgresSettings(
        pguser=database.user,
        pgpassword=database.password,
        pghost=database.host,
        pgport=database.port,
        pgdatabase=database.dbname,
    )
    logger.info("Creating app Fixture")
    time.time()
    app = api_client.app
    await connect_to_db(
        app,
        postgres_settings=postgres_settings,
        add_write_connection_pool=True,
    )

    yield app

    await close_db_connection(app)

    logger.info("Closed Pools.")


@pytest.fixture(scope="function")
async def app_client(app):
    logger.info("creating app_client")

    base_url = "http://test"
    if app.state.router_prefix != "":
        base_url = urljoin(base_url, app.state.router_prefix)

    async with AsyncClient(transport=ASGITransport(app=app), base_url=base_url) as c:
        yield c


@pytest.fixture
def load_test_data() -> Callable[[str], Dict]:
    def load_file(filename: str) -> Dict:
        with open(os.path.join(DATA_DIR, filename)) as file:
            return json.load(file)

    return load_file


@pytest.fixture
async def load_test_collection(app_client, load_test_data):
    data = load_test_data("test_collection.json")
    resp = await app_client.post(
        "/collections",
        json=data,
    )
    assert resp.status_code == 201
    collection = Collection.model_validate(resp.json())

    return collection.model_dump(mode="json")


@pytest.fixture
async def load_test_item(app_client, load_test_data, load_test_collection):
    coll = load_test_collection
    data = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{coll['id']}/items",
        json=data,
    )
    assert resp.status_code == 201

    item = Item.model_validate(resp.json())
    return item.model_dump(mode="json")


@pytest.fixture
async def load_test2_collection(app_client, load_test_data):
    data = load_test_data("test2_collection.json")
    resp = await app_client.post(
        "/collections",
        json=data,
    )
    assert resp.status_code == 201
    return Collection.model_validate(resp.json())


@pytest.fixture
async def load_test2_item(app_client, load_test_data, load_test2_collection):
    coll = load_test2_collection
    data = load_test_data("test2_item.json")
    resp = await app_client.post(
        f"/collections/{coll.id}/items",
        json=data,
    )
    assert resp.status_code == 201
    return Item.model_validate(resp.json())


@pytest.fixture(scope="function")
async def app_no_ext(database):
    """Default stac-fastapi-pgstac application without only the transaction extensions."""
    api_settings = Settings(testing=True)
    api_client_no_ext = StacApi(
        settings=api_settings,
        extensions=[
            TransactionExtension(client=TransactionsClient(), settings=api_settings)
        ],
        client=CoreCrudClient(),
        health_check=health_check,
    )

    postgres_settings = PostgresSettings(
        pguser=database.user,
        pgpassword=database.password,
        pghost=database.host,
        pgport=database.port,
        pgdatabase=database.dbname,
    )
    logger.info("Creating app Fixture")
    time.time()
    await connect_to_db(
        api_client_no_ext.app,
        postgres_settings=postgres_settings,
        add_write_connection_pool=True,
    )
    yield api_client_no_ext.app
    await close_db_connection(api_client_no_ext.app)

    logger.info("Closed Pools.")


@pytest.fixture(scope="function")
async def app_client_no_ext(app_no_ext):
    logger.info("creating app_client")
    async with AsyncClient(
        transport=ASGITransport(app=app_no_ext), base_url="http://test"
    ) as c:
        yield c


@pytest.fixture(scope="function")
async def app_no_transaction(database):
    """Default stac-fastapi-pgstac application without any extensions."""
    api_settings = Settings(testing=True)
    api = StacApi(
        settings=api_settings,
        extensions=[],
        client=CoreCrudClient(),
        health_check=health_check,
    )

    postgres_settings = PostgresSettings(
        pguser=database.user,
        pgpassword=database.password,
        pghost=database.host,
        pgport=database.port,
        pgdatabase=database.dbname,
    )
    logger.info("Creating app Fixture")
    time.time()
    await connect_to_db(
        api.app,
        postgres_settings=postgres_settings,
        add_write_connection_pool=False,
    )
    yield api.app
    await close_db_connection(api.app)

    logger.info("Closed Pools.")


@pytest.fixture(scope="function")
async def app_client_no_transaction(app_no_transaction):
    logger.info("creating app_client")
    async with AsyncClient(
        transport=ASGITransport(app=app_no_transaction), base_url="http://test"
    ) as c:
        yield c


@pytest.fixture(scope="function")
async def default_app(database, monkeypatch):
    """Test default stac-fastapi-pgstac application."""
    monkeypatch.setenv("PGUSER", database.user)
    monkeypatch.setenv("PGPASSWORD", database.password)
    monkeypatch.setenv("PGHOST", database.host)
    monkeypatch.setenv("PGPORT", str(database.port))
    monkeypatch.setenv("PGDATABASE", database.dbname)
    monkeypatch.delenv("ENABLED_EXTENSIONS", raising=False)

    monkeypatch.setenv("ENABLE_TRANSACTIONS_EXTENSIONS", "TRUE")
    monkeypatch.setenv("USE_API_HYDRATE", "TRUE")
    monkeypatch.setenv("ENABLE_RESPONSE_MODELS", "TRUE")

    from stac_fastapi.pgstac.app import app

    await connect_to_db(app, add_write_connection_pool=True)
    yield app
    await close_db_connection(app)


@pytest.fixture(scope="function")
async def default_client(default_app):
    async with AsyncClient(
        transport=ASGITransport(app=default_app), base_url="http://test"
    ) as c:
        yield c
