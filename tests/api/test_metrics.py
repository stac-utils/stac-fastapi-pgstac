from contextlib import asynccontextmanager

from httpx import ASGITransport, AsyncClient

from stac_fastapi.pgstac.app import instantiate_api
from stac_fastapi.pgstac.config import Settings


@asynccontextmanager
async def noop_lifespan(app):
    """Lifespan that skips connecting to the database."""
    yield


async def test_metrics_disabled_by_default():
    api = instantiate_api(settings=Settings(testing=True), lifespan=noop_lifespan)

    async with AsyncClient(
        transport=ASGITransport(app=api.app), base_url="http://test"
    ) as client:
        res = await client.get("/_mgmt/metrics")
        assert res.status_code == 404


async def test_metrics_enabled():
    api = instantiate_api(
        settings=Settings(testing=True, enable_metrics=True),
        lifespan=noop_lifespan,
    )

    async with AsyncClient(
        transport=ASGITransport(app=api.app), base_url="http://test"
    ) as client:
        await client.get("/_mgmt/ping")
        res = await client.get("/_mgmt/metrics")
        assert res.status_code == 200
        assert "http_requests_total" in res.text
