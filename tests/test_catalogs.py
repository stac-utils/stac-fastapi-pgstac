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

    # Verify parent_ids is NOT exposed in the response (internal only)
    assert "parent_ids" not in retrieved_sub

    # Verify links structure
    assert "links" in retrieved_sub
    links = retrieved_sub["links"]

    # Check for parent link (generated from parent_ids)
    parent_links = [link for link in links if link.get("rel") == "parent"]
    assert len(parent_links) > 0
    parent_link = parent_links[0]
    assert "parent-for-links" in parent_link.get("href", "")

    # Check for root link
    root_links = [link for link in links if link.get("rel") == "root"]
    assert len(root_links) > 0


@pytest.mark.asyncio
async def test_catalog_links_parent_and_root(app_client):
    """Test that a catalog has proper parent and root links."""
    # Create a parent catalog
    parent_catalog = {
        "id": "parent-catalog-links",
        "type": "Catalog",
        "description": "Parent catalog for link tests",
        "stac_version": "1.0.0",
        "links": [],
    }
    resp = await app_client.post("/catalogs", json=parent_catalog)
    assert resp.status_code == 201

    # Get the parent catalog
    resp = await app_client.get("/catalogs/parent-catalog-links")
    assert resp.status_code == 200
    parent = resp.json()
    parent_links = parent.get("links", [])

    # Check for self link
    self_links = [link for link in parent_links if link.get("rel") == "self"]
    assert len(self_links) == 1
    assert "parent-catalog-links" in self_links[0]["href"]

    # Check for parent link (should point to root)
    parent_rel_links = [link for link in parent_links if link.get("rel") == "parent"]
    assert len(parent_rel_links) == 1
    assert parent_rel_links[0]["title"] == "Root Catalog"

    # Check for root link
    root_links = [link for link in parent_links if link.get("rel") == "root"]
    assert len(root_links) == 1


@pytest.mark.asyncio
async def test_catalog_child_links(app_client):
    """Test that a catalog with children has proper child links."""
    # Create a parent catalog
    parent_catalog = {
        "id": "parent-with-children",
        "type": "Catalog",
        "description": "Parent catalog with children",
        "stac_version": "1.0.0",
        "links": [],
    }
    resp = await app_client.post("/catalogs", json=parent_catalog)
    assert resp.status_code == 201

    # Create child catalogs
    child_ids = ["child-1", "child-2"]
    for child_id in child_ids:
        child_catalog = {
            "id": child_id,
            "type": "Catalog",
            "description": f"Child catalog {child_id}",
            "stac_version": "1.0.0",
            "links": [],
        }
        resp = await app_client.post(
            "/catalogs/parent-with-children/catalogs",
            json=child_catalog,
        )
        assert resp.status_code == 201

    # Get the parent catalog
    resp = await app_client.get("/catalogs/parent-with-children")
    assert resp.status_code == 200
    parent = resp.json()
    parent_links = parent.get("links", [])

    # Check for child links
    child_links = [link for link in parent_links if link.get("rel") == "child"]
    assert len(child_links) == 2

    # Verify child link hrefs
    child_hrefs = [link["href"] for link in child_links]
    for child_id in child_ids:
        assert any(child_id in href for href in child_hrefs)


@pytest.mark.asyncio
async def test_nested_catalog_parent_link(app_client):
    """Test that a nested catalog has proper parent link pointing to its parent."""
    # Create a parent catalog
    parent_catalog = {
        "id": "grandparent-catalog",
        "type": "Catalog",
        "description": "Grandparent catalog",
        "stac_version": "1.0.0",
        "links": [],
    }
    resp = await app_client.post("/catalogs", json=parent_catalog)
    assert resp.status_code == 201

    # Create a child catalog
    child_catalog = {
        "id": "child-of-grandparent",
        "type": "Catalog",
        "description": "Child of grandparent",
        "stac_version": "1.0.0",
        "links": [],
    }
    resp = await app_client.post(
        "/catalogs/grandparent-catalog/catalogs",
        json=child_catalog,
    )
    assert resp.status_code == 201

    # Get the child catalog
    resp = await app_client.get("/catalogs/child-of-grandparent")
    assert resp.status_code == 200
    child = resp.json()
    child_links = child.get("links", [])

    # Check for parent link pointing to grandparent
    parent_links = [link for link in child_links if link.get("rel") == "parent"]
    assert len(parent_links) == 1
    assert "grandparent-catalog" in parent_links[0]["href"]
    assert parent_links[0]["title"] == "grandparent-catalog"


@pytest.mark.asyncio
async def test_catalog_links_use_correct_base_url(app_client):
    """Test that catalog links use the correct base URL."""
    # Create a catalog
    catalog_data = {
        "id": "base-url-test",
        "type": "Catalog",
        "description": "Test catalog for base URL",
        "stac_version": "1.0.0",
        "links": [],
    }
    resp = await app_client.post("/catalogs", json=catalog_data)
    assert resp.status_code == 201

    # Get the catalog
    resp = await app_client.get("/catalogs/base-url-test")
    assert resp.status_code == 200
    catalog = resp.json()
    links = catalog.get("links", [])

    # Check that we have the expected link types
    link_rels = [link.get("rel") for link in links]
    assert "self" in link_rels
    assert "parent" in link_rels
    assert "root" in link_rels

    # Check that links are properly formed
    for link in links:
        href = link.get("href", "")
        assert href, f"Link {link.get('rel')} has no href"
        # Links should be either absolute or relative
        assert href.startswith("/") or href.startswith("http")


@pytest.mark.asyncio
async def test_parent_ids_not_exposed_in_response(app_client):
    """Test that parent_ids is not exposed in the API response."""
    # Create a parent catalog
    parent_catalog = {
        "id": "parent-for-exposure-test",
        "type": "Catalog",
        "description": "Parent catalog",
        "stac_version": "1.0.0",
        "links": [],
    }
    resp = await app_client.post("/catalogs", json=parent_catalog)
    assert resp.status_code == 201

    # Create a child catalog
    child_catalog = {
        "id": "child-for-exposure-test",
        "type": "Catalog",
        "description": "Child catalog",
        "stac_version": "1.0.0",
        "links": [],
    }
    resp = await app_client.post(
        "/catalogs/parent-for-exposure-test/catalogs",
        json=child_catalog,
    )
    assert resp.status_code == 201

    # Get the child catalog
    resp = await app_client.get("/catalogs/child-for-exposure-test")
    assert resp.status_code == 200
    catalog = resp.json()

    # Verify that parent_ids is NOT in the response
    assert "parent_ids" not in catalog, "parent_ids should not be exposed in API response"

    # Verify that parent link is still present (generated from parent_ids)
    parent_links = [
        link for link in catalog.get("links", []) if link.get("rel") == "parent"
    ]
    assert len(parent_links) == 1
    assert "parent-for-exposure-test" in parent_links[0]["href"]
