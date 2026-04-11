"""Tests for the catalogs extension."""

import logging

import pytest

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_create_catalog(app_client):
    """Test creating a catalog."""

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


@pytest.mark.asyncio
async def test_create_sub_catalog(app_client):
    """Test creating a sub-catalog."""
    # First create a parent catalog
    parent_catalog_data = {
        "id": "parent-catalog",
        "type": "Catalog",
        "description": "A parent catalog",
        "stac_version": "1.0.0",
        "links": [],
    }

    resp = await app_client.post(
        "/catalogs",
        json=parent_catalog_data,
    )
    assert resp.status_code == 201

    # Now create a sub-catalog
    sub_catalog_data = {
        "id": "sub-catalog-1",
        "type": "Catalog",
        "description": "A sub-catalog",
        "stac_version": "1.0.0",
        "links": [],
    }

    resp = await app_client.post(
        "/catalogs/parent-catalog/catalogs",
        json=sub_catalog_data,
    )
    assert resp.status_code == 201
    created_sub_catalog = resp.json()
    assert created_sub_catalog["id"] == "sub-catalog-1"
    assert created_sub_catalog["type"] == "Catalog"
    assert "parent_ids" in created_sub_catalog
    assert "parent-catalog" in created_sub_catalog["parent_ids"]


@pytest.mark.asyncio
async def test_get_sub_catalogs(app_client):
    """Test getting sub-catalogs of a parent catalog."""
    # Create a parent catalog
    parent_catalog_data = {
        "id": "parent-catalog-2",
        "type": "Catalog",
        "description": "A parent catalog for sub-catalogs",
        "stac_version": "1.0.0",
        "links": [],
    }

    resp = await app_client.post(
        "/catalogs",
        json=parent_catalog_data,
    )
    assert resp.status_code == 201

    # Create multiple sub-catalogs
    sub_catalog_ids = ["sub-cat-1", "sub-cat-2", "sub-cat-3"]
    for sub_id in sub_catalog_ids:
        sub_catalog_data = {
            "id": sub_id,
            "type": "Catalog",
            "description": f"Sub-catalog {sub_id}",
            "stac_version": "1.0.0",
            "links": [],
        }

        resp = await app_client.post(
            "/catalogs/parent-catalog-2/catalogs",
            json=sub_catalog_data,
        )
        assert resp.status_code == 201

    # Get all sub-catalogs
    resp = await app_client.get("/catalogs/parent-catalog-2/catalogs")
    assert resp.status_code == 200
    data = resp.json()
    assert "catalogs" in data
    assert isinstance(data["catalogs"], list)
    assert len(data["catalogs"]) >= 3

    # Check that all sub-catalogs are in the list
    returned_sub_ids = [cat.get("id") for cat in data["catalogs"]]
    for sub_id in sub_catalog_ids:
        assert sub_id in returned_sub_ids

    # Verify links structure
    assert "links" in data
    links = data["links"]
    assert len(links) > 0

    # Check for required link relations
    link_rels = [link.get("rel") for link in links]
    assert "root" in link_rels
    assert "parent" in link_rels
    assert "self" in link_rels

    # Verify self link points to the correct endpoint
    self_link = next((link for link in links if link.get("rel") == "self"), None)
    assert self_link is not None
    assert "/catalogs/parent-catalog-2/catalogs" in self_link.get("href", "")


@pytest.mark.asyncio
async def test_sub_catalog_links(app_client):
    """Test that sub-catalogs have correct parent links."""
    # Create a parent catalog
    parent_catalog_data = {
        "id": "parent-for-links",
        "type": "Catalog",
        "description": "Parent catalog for link testing",
        "stac_version": "1.0.0",
        "links": [],
    }

    resp = await app_client.post(
        "/catalogs",
        json=parent_catalog_data,
    )
    assert resp.status_code == 201

    # Create a sub-catalog
    sub_catalog_data = {
        "id": "sub-for-links",
        "type": "Catalog",
        "description": "Sub-catalog for link testing",
        "stac_version": "1.0.0",
        "links": [],
    }

    resp = await app_client.post(
        "/catalogs/parent-for-links/catalogs",
        json=sub_catalog_data,
    )
    assert resp.status_code == 201

    # Get the sub-catalog directly
    resp = await app_client.get("/catalogs/sub-for-links")
    assert resp.status_code == 200
    retrieved_sub = resp.json()

    # Verify parent_ids
    assert "parent_ids" in retrieved_sub
    assert "parent-for-links" in retrieved_sub["parent_ids"]

    # Verify links structure
    assert "links" in retrieved_sub
    links = retrieved_sub["links"]

    # Check for parent link
    parent_links = [link for link in links if link.get("rel") == "parent"]
    assert len(parent_links) > 0
    parent_link = parent_links[0]
    assert "parent-for-links" in parent_link.get("href", "")

    # Check for root link
    root_links = [link for link in links if link.get("rel") == "root"]
    assert len(root_links) > 0
