from urllib.parse import urljoin

import pytest
from httpx import ASGITransport, AsyncClient

from stac_fastapi.pgstac.metrics import metrics_endpoint


@pytest.fixture
async def metrics_app(app):
    """Reuse the shared app fixture (already instrumented at construction)."""
    yield app


@pytest.fixture
async def metrics_client(metrics_app):
    prefix = metrics_app.state.router_prefix
    stac_base_url = "http://test"
    if prefix:
        stac_base_url = urljoin(stac_base_url, prefix)

    async with AsyncClient(
        transport=ASGITransport(app=metrics_app),
        base_url=stac_base_url,
    ) as client:
        yield client


def _request_total_lines(body: str) -> list[str]:
    return [line for line in body.splitlines() if line.startswith("http_requests_total{")]


async def _fetch_metrics(app) -> str:
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        resp = await client.get(metrics_endpoint(app))
    assert resp.status_code == 200
    return resp.text


async def test_metrics_endpoint(metrics_app):
    body = await _fetch_metrics(metrics_app)

    assert "http_requests_total" in body
    assert "http_request_duration_seconds" in body


async def test_metrics_endpoint_respects_router_prefix(metrics_app):
    endpoint = metrics_endpoint(metrics_app)

    async with AsyncClient(
        transport=ASGITransport(app=metrics_app),
        base_url="http://test",
    ) as client:
        assert (await client.get("/metrics")).status_code == 404
        resp = await client.get(endpoint)

    assert resp.status_code == 200
    if metrics_app.state.router_prefix:
        assert endpoint.startswith(metrics_app.state.router_prefix)
    assert endpoint.endswith("/_mgmt/metrics")


async def test_metrics_use_operation_labels(metrics_client, load_test_data):
    collection = load_test_data("test_collection.json")
    item_a = load_test_data("test_item.json")
    item_b = load_test_data("test2_item.json")

    resp = await metrics_client.post("/collections", json=collection)
    assert resp.status_code == 201
    collection_id = collection["id"]

    await metrics_client.get("/search", params={"limit": 1})
    await metrics_client.get(f"/collections/{collection_id}/items")
    await metrics_client.get(f"/collections/{collection_id}/items/{item_a['id']}")
    await metrics_client.get(f"/collections/{collection_id}/items/{item_b['id']}")

    body = await _fetch_metrics(metrics_client._transport.app)

    assert 'operation="search"' in body
    assert 'operation="list_items"' in body
    assert 'operation="get_item"' in body
    assert 'operation="create_collection"' in body
    assert item_a["id"] not in body
    assert item_b["id"] not in body

    totals = _request_total_lines(body)
    get_item_lines = [line for line in totals if 'operation="get_item"' in line]
    assert get_item_lines
    assert all(item_a["id"] not in line for line in get_item_lines)
    assert all(item_b["id"] not in line for line in get_item_lines)
