from httpx import ASGITransport, AsyncClient
from stac_fastapi.api.app import StacApi

from stac_fastapi.pgstac.config import PostgresSettings, Settings
from stac_fastapi.pgstac.core import CoreCrudClient, health_check
from stac_fastapi.pgstac.db import close_db_connection, connect_to_db


async def test_ping_no_param(app_client):
    """
    Test ping endpoint with a mocked client.
    Args:
        app_client (TestClient): mocked client fixture
    """
    res = await app_client.get("/_mgmt/ping")
    assert res.status_code == 200
    assert res.json() == {"message": "PONG"}


async def test_health(app_client):
    """
    Test health endpoint

    Args:
        app_client (TestClient): mocked client fixture

    """
    res = await app_client.get("/_mgmt/health")
    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "UP"
    assert body["pgstac"]["status"] == "UP"
    assert body["pgstac"]["pgstac_version"]


async def test_health_503(database):
    """Test health endpoint error."""

    # No lifespan so no `get_connection` is application state
    api = StacApi(
        settings=Settings(testing=True),
        extensions=[],
        client=CoreCrudClient(),
        health_check=health_check,
    )

    async with AsyncClient(
        transport=ASGITransport(app=api.app), base_url="http://test"
    ) as client:
        res = await client.get("/_mgmt/health")
        assert res.status_code == 503
        body = res.json()
        assert body["status"] == "DOWN"
        assert body["lifespan"]["status"] == "DOWN"
        assert body["lifespan"]["message"] == "application lifespan wasn't run"
        assert body["pgstac"]["status"] == "DOWN"
        assert body["pgstac"]["message"] == "Could not connect to database"

    # No lifespan so no `get_connection` is application state
    postgres_settings = PostgresSettings(
        pguser=database.user,
        pgpassword=database.password,
        pghost=database.host,
        pgport=database.port,
        pgdatabase=database.dbname,
    )
    # Create connection pool but close it just after
    await connect_to_db(api.app, postgres_settings=postgres_settings)
    await close_db_connection(api.app)

    async with AsyncClient(
        transport=ASGITransport(app=api.app), base_url="http://test"
    ) as client:
        res = await client.get("/_mgmt/health")
        assert res.status_code == 503
        body = res.json()
        assert body["status"] == "DOWN"
        assert body["lifespan"]["status"] == "UP"
        assert body["pgstac"]["status"] == "DOWN"
        assert body["pgstac"]["message"] == "pool is closed"
