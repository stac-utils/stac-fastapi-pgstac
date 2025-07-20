import importlib

import pytest
from starlette.testclient import TestClient

from stac_fastapi.pgstac.db import close_db_connection, connect_to_db

BASE_URL = "http://api.acme.com"
ROOT_PATH = "/stac/v1"


@pytest.fixture(scope="function")
async def app_with_root_path(database, monkeypatch):
    """
    Provides the global stac_fastapi.pgstac.app.app instance, configured with a
    specific ROOT_PATH environment variable and connected to the test database.
    """

    monkeypatch.setenv("ROOT_PATH", ROOT_PATH)
    monkeypatch.setenv("PGUSER", database.user)
    monkeypatch.setenv("PGPASSWORD", database.password)
    monkeypatch.setenv("PGHOST", database.host)
    monkeypatch.setenv("PGPORT", str(database.port))
    monkeypatch.setenv("PGDATABASE", database.dbname)
    monkeypatch.setenv("ENABLE_TRANSACTIONS_EXTENSIONS", "TRUE")

    # Reload the app module to pick up the new environment variables
    import stac_fastapi.pgstac.app

    importlib.reload(stac_fastapi.pgstac.app)

    from stac_fastapi.pgstac.app import app, with_transactions

    # Ensure the app's root_path is configured as expected
    assert (
        app.root_path == ROOT_PATH
    ), f"app_with_root_path fixture: app.root_path is '{app.root_path}', expected '{ROOT_PATH}'"

    await connect_to_db(app, add_write_connection_pool=with_transactions)
    yield app
    await close_db_connection(app)


@pytest.fixture(scope="function")
def client_with_root_path(app_with_root_path):
    with TestClient(
        app_with_root_path,
        base_url=BASE_URL,
        root_path=ROOT_PATH,
    ) as c:
        yield c


@pytest.fixture(scope="function")
def loaded_client(client_with_root_path, load_test_data):
    col = load_test_data("test_collection.json")
    resp = client_with_root_path.post(
        "/collections",
        json=col,
    )
    assert resp.status_code == 201
    item = load_test_data("test_item.json")
    resp = client_with_root_path.post(
        f"/collections/{col['id']}/items",
        json=item,
    )
    assert resp.status_code == 201
    item = load_test_data("test_item2.json")
    resp = client_with_root_path.post(
        f"/collections/{col['id']}/items",
        json=item,
    )
    assert resp.status_code == 201
    yield client_with_root_path


@pytest.mark.parametrize(
    "path",
    [
        "/search?limit=1",
        "/collections?limit=1",
        "/collections/test-collection/items?limit=1",
    ],
)
def test_search_links_are_valid(loaded_client, path):
    resp = loaded_client.get(path)
    assert resp.status_code == 200
    response_json = resp.json()

    # Ensure all links start with the expected URL prefix and check that
    # there is no root_path duplicated in the URL.
    failed_links = []
    expected_prefix = f"{BASE_URL}{ROOT_PATH}"

    for link in response_json.get("links", []):
        href = link["href"]
        rel = link.get("rel", "unknown")

        # Check if link starts with the expected prefix
        if not href.startswith(expected_prefix):
            failed_links.append(
                {
                    "rel": rel,
                    "href": href,
                    "error": f"does not start with expected prefix '{expected_prefix}'",
                }
            )
            continue

        # Check for duplicated root path
        remainder = href[len(expected_prefix) :]
        if remainder.startswith(ROOT_PATH):
            failed_links.append(
                {
                    "rel": rel,
                    "href": href,
                    "error": f"contains duplicated root path '{ROOT_PATH}'",
                }
            )

    # If there are failed links, create a detailed error report
    if failed_links:
        error_report = "Link validation failed:\n"
        for failed_link in failed_links:
            error_report += f"  - rel: '{failed_link['rel']}', href: '{failed_link['href']}' - {failed_link['error']}\n"

        raise AssertionError(error_report)
