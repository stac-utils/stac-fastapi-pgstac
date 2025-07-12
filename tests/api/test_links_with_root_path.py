import pytest
from httpx import ASGITransport, AsyncClient

from stac_fastapi.pgstac.db import close_db_connection, connect_to_db

root_path_value = "/stac/v1"

# Append the root path to the base URL, this is key to reproducing the issue where the root path appears twice in some links
base_url = f"http://api.acme.com{root_path_value}"


@pytest.fixture(scope="function")
async def app_with_root_path(database, monkeypatch):
    """
    Provides the global stac_fastapi.pgstac.app.app instance, configured with a
    specific ROOT_PATH environment variable and connected to the test database.
    """

    monkeypatch.setenv("ROOT_PATH", root_path_value)
    monkeypatch.setenv("PGUSER", database.user)
    monkeypatch.setenv("PGPASSWORD", database.password)
    monkeypatch.setenv("PGHOST", database.host)
    monkeypatch.setenv("PGPORT", str(database.port))
    monkeypatch.setenv("PGDATABASE", database.dbname)
    monkeypatch.setenv("ENABLE_TRANSACTIONS_EXTENSIONS", "TRUE")

    from stac_fastapi.pgstac.app import app, with_transactions

    # Ensure the app's root_path is configured as expected
    assert (
        app.root_path == root_path_value
    ), f"app_with_root_path fixture: app.root_path is '{app.root_path}', expected '{root_path_value}'"

    await connect_to_db(app, add_write_connection_pool=with_transactions)
    yield app
    await close_db_connection(app)


@pytest.fixture(scope="function")
async def client_with_root_path(app_with_root_path):
    async with AsyncClient(
        transport=ASGITransport(app=app_with_root_path),
        base_url=base_url,
    ) as c:
        yield c


@pytest.fixture(scope="function")
async def loaded_client(client_with_root_path, load_test_data):
    col = load_test_data("test_collection.json")
    resp = await client_with_root_path.post(
        "/collections",
        json=col,
    )
    assert resp.status_code == 201
    item = load_test_data("test_item.json")
    resp = await client_with_root_path.post(
        f"/collections/{col['id']}/items",
        json=item,
    )
    assert resp.status_code == 201
    item = load_test_data("test_item2.json")
    resp = await client_with_root_path.post(
        f"/collections/{col['id']}/items",
        json=item,
    )
    assert resp.status_code == 201
    yield client_with_root_path


async def test_search_links_are_valid(loaded_client):
    resp = await loaded_client.get("/search?limit=1")
    assert resp.status_code == 200
    response_json = resp.json()
    assert_links_href(response_json.get("links", []), base_url)


async def test_collection_links_are_valid(loaded_client):
    resp = await loaded_client.get("/collections?limit=1")
    assert resp.status_code == 200
    response_json = resp.json()
    assert_links_href(response_json.get("links", []), base_url)


def assert_links_href(links, url_prefix):
    """
    Ensure all links start with the expected URL prefix and check that
    there is no root_path duplicated in the URL.

    Args:
        links: List of link dictionaries with 'href' keys
        url_prefix: Expected URL prefix (e.g., 'http://test/stac/v1')
    """
    from urllib.parse import urlparse

    failed_links = []
    parsed_prefix = urlparse(url_prefix)
    root_path = parsed_prefix.path  # e.g., '/stac/v1'

    for link in links:
        href = link["href"]
        rel = link.get("rel", "unknown")

        # Check if link starts with the expected prefix
        if not href.startswith(url_prefix):
            failed_links.append(
                {
                    "rel": rel,
                    "href": href,
                    "error": f"does not start with expected prefix '{url_prefix}'",
                }
            )
            continue

        # Check for duplicated root path
        if root_path and root_path != "/":
            remainder = href[len(url_prefix) :]
            if remainder.startswith(root_path):
                failed_links.append(
                    {
                        "rel": rel,
                        "href": href,
                        "error": f"contains duplicated root path '{root_path}'",
                    }
                )

    # If there are failed links, create a detailed error report
    if failed_links:
        error_report = "Link validation failed:\n"
        for failed_link in failed_links:
            error_report += f"  - rel: '{failed_link['rel']}', href: '{failed_link['href']}' - {failed_link['error']}\n"

        raise AssertionError(error_report)
