"""Tests for the catalogs extension."""

import logging
from urllib.parse import urlparse

import pytest

logger = logging.getLogger(__name__)


def has_router_prefix(app_client):
    """Check if the app_client has a router prefix."""
    parsed = urlparse(str(app_client.base_url))
    return "/router_prefix" in parsed.path


@pytest.mark.asyncio
async def test_create_catalog(app_client):
    """Test creating a catalog."""
    if has_router_prefix(app_client):
        pytest.skip("Catalogs extension routes not registered with router prefix")

    catalog_data = {
        "id": "test-catalog",
        "type": "Catalog",
        "description": "A test catalog",
        "stac_version": "1.0.0",
        "links": [],
    }

    resp = await app_client.post(
        "/catalogs",
        json=catalog_data,
    )
    assert resp.status_code == 201
    created_catalog = resp.json()
    assert created_catalog["id"] == "test-catalog"
    assert created_catalog["type"] == "Catalog"
    assert created_catalog["description"] == "A test catalog"


@pytest.mark.asyncio
async def test_get_all_catalogs(app_client):
    """Test getting all catalogs."""
    if has_router_prefix(app_client):
        pytest.skip("Catalogs extension routes not registered with router prefix")

    # Create three catalogs
    catalog_ids = ["test-catalog-1", "test-catalog-2", "test-catalog-3"]
    for catalog_id in catalog_ids:
        catalog_data = {
            "id": catalog_id,
            "type": "Catalog",
            "description": f"Test catalog {catalog_id}",
            "stac_version": "1.0.0",
            "links": [],
        }

        resp = await app_client.post(
            "/catalogs",
            json=catalog_data,
        )
        assert resp.status_code == 201

    # Now get all catalogs
    resp = await app_client.get("/catalogs")
    assert resp.status_code == 200
    data = resp.json()
    assert "catalogs" in data
    assert isinstance(data["catalogs"], list)
    assert len(data["catalogs"]) >= 3

    # Check that all three created catalogs are in the list
    returned_catalog_ids = [cat.get("id") for cat in data["catalogs"]]
    for catalog_id in catalog_ids:
        assert catalog_id in returned_catalog_ids


@pytest.mark.asyncio
async def test_get_catalog_by_id(app_client):
    """Test getting a specific catalog by ID."""
    if has_router_prefix(app_client):
        pytest.skip("Catalogs extension routes not registered with router prefix")

    # First create a catalog
    catalog_data = {
        "id": "test-catalog-get",
        "type": "Catalog",
        "description": "A test catalog for getting",
        "stac_version": "1.0.0",
        "links": [],
    }

    resp = await app_client.post(
        "/catalogs",
        json=catalog_data,
    )
    assert resp.status_code == 201

    # Now get the specific catalog
    resp = await app_client.get("/catalogs/test-catalog-get")
    assert resp.status_code == 200
    retrieved_catalog = resp.json()
    assert retrieved_catalog["id"] == "test-catalog-get"
    assert retrieved_catalog["type"] == "Catalog"
    assert retrieved_catalog["description"] == "A test catalog for getting"


@pytest.mark.asyncio
async def test_get_nonexistent_catalog(app_client):
    """Test getting a catalog that doesn't exist."""
    resp = await app_client.get("/catalogs/nonexistent-catalog-id")
    assert resp.status_code == 404
