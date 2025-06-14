import os
from unittest import mock
from urllib.parse import urlparse

import pytest
from fastapi import APIRouter, FastAPI
from starlette.requests import Request
from starlette.testclient import TestClient

# Assuming app is defined in stac_fastapi.pgstac.app
# If not, this import will need adjustment.
from stac_fastapi.pgstac.app import app
from stac_fastapi.pgstac.models import links as app_links


@pytest.mark.parametrize("root_path", ["", "/api/v1"])
@pytest.mark.parametrize("prefix", ["", "/stac"])
def tests_app_links(prefix, root_path):  # noqa: C901
    endpoint_prefix = root_path + prefix
    url_prefix = "http://stac.io" + endpoint_prefix

    app = FastAPI(root_path=root_path)
    router = APIRouter(prefix=prefix)
    app.state.router_prefix = router.prefix

    @router.get("/search")
    @router.post("/search")
    async def search(request: Request):
        links = app_links.PagingLinks(request, next="yo:2", prev="yo:1")
        return {
            "url": links.url,
            "base_url": links.base_url,
            "links": await links.get_links(),
        }

    @router.get("/collections")
    async def collections(request: Request):
        pgstac_next = {
            "rel": "next",
            "body": {"offset": 1},
            "href": "./collections",
            "type": "application/json",
            "merge": True,
            "method": "GET",
        }
        pgstac_prev = {
            "rel": "prev",
            "body": {"offset": 0},
            "href": "./collections",
            "type": "application/json",
            "merge": True,
            "method": "GET",
        }
        links = app_links.CollectionSearchPagingLinks(
            request, next=pgstac_next, prev=pgstac_prev
        )
        return {
            "url": links.url,
            "base_url": links.base_url,
            "links": await links.get_links(),
        }

    app.include_router(router)

    with TestClient(
        app,
        base_url="http://stac.io",
        root_path=root_path,
    ) as client:
        response = client.get(f"{prefix}/search")
        assert response.status_code == 200
        assert response.json()["url"] == url_prefix + "/search"
        assert response.json()["base_url"].rstrip("/") == url_prefix
        links = response.json()["links"]
        for link in links:
            if link["rel"] in ["previous", "next"]:
                assert link["method"] == "GET"
            assert link["href"].startswith(url_prefix)
        assert {"next", "previous", "root", "self"} == {link["rel"] for link in links}


# The load_test_data fixture is assumed to be defined in conftest.py
# and to load enough items for pagination to occur with limit=1.
@mock.patch.dict(os.environ, {"ROOT_PATH": "/custom/api/root"})
def test_pagination_link_with_root_path(load_test_data):
    """Test that pagination links are correct when ROOT_PATH is set."""
    # get_base_url directly uses os.getenv("ROOT_PATH"), so patching
    # os.environ should be effective for new requests.
    # The TestClient is initialized after the patch.

    # Use the global `app` imported from stac_fastapi.pgstac.app
    # The TestClient for STAC FastAPI typically uses http://testserver as base
    with TestClient(app, base_url="http://testserver") as client:
        # Perform a search that should result in a 'next' link
        # Assumes load_test_data has loaded more than 1 item
        response = client.get("/search?limit=1")
        response_json = response.json()

        assert response.status_code == 200, f"Response content: {response.text}"
        next_link = None
        for link in response_json.get("links", []):
            if link.get("rel") == "next":
                next_link = link
                break

        assert next_link is not None, "Next link not found in response"

        href = next_link["href"]

        # Expected: http://testserver/custom/api/root/search?limit=1&token=next:...
        # Not: http://testserver/custom/api/root/custom/api/root/search?...

        parsed_href = urlparse(href)
        path = parsed_href.path

        # Check that the path starts correctly with the root path and the endpoint
        expected_start_path = "/custom/api/root/search"
        assert path.startswith(expected_start_path), f"Path {path} does not start with {expected_start_path}"

        # Check that the root path segment is not duplicated
        # e.g. path should not be /custom/api/root/custom/api/root/search
        duplicated_root_path = "/custom/api/root/custom/api/root/"
        assert not path.startswith(duplicated_root_path), f"Path {path} shows duplicated root path starting with {duplicated_root_path}"

        # A more precise check for occurrences of the root path segments
        # Path: /custom/api/root/search
        # Root Path: /custom/api/root
        # Effective path segments to check for duplication: custom/api/root
        path_segments = path.strip('/').split('/')
        root_path_segments_to_check = "custom/api/root".split('/')

        occurrences = 0
        for i in range(len(path_segments) - len(root_path_segments_to_check) + 1):
            if path_segments[i:i+len(root_path_segments_to_check)] == root_path_segments_to_check:
                occurrences += 1

        assert occurrences == 1, f"Expected ROOT_PATH segments to appear once, but found {occurrences} times in path {path}. Segments: {path_segments}"

        response = client.get(f"{prefix}/search", params={"limit": 1})
        assert response.status_code == 200
        assert response.json()["url"] == url_prefix + "/search?limit=1"
        assert response.json()["base_url"].rstrip("/") == url_prefix
        links = response.json()["links"]
        for link in links:
            if link["rel"] in ["previous", "next"]:
                assert link["method"] == "GET"
                assert "limit=1" in link["href"]
            assert link["href"].startswith(url_prefix)
        assert {"next", "previous", "root", "self"} == {link["rel"] for link in links}

        response = client.post(f"{prefix}/search", json={})
        assert response.status_code == 200
        assert response.json()["url"] == url_prefix + "/search"
        assert response.json()["base_url"].rstrip("/") == url_prefix
        links = response.json()["links"]
        for link in links:
            if link["rel"] in ["previous", "next"]:
                assert link["method"] == "POST"
            assert link["href"].startswith(url_prefix)
        assert {"next", "previous", "root", "self"} == {link["rel"] for link in links}

        response = client.get(f"{prefix}/collections")
        assert response.status_code == 200
        assert response.json()["url"] == url_prefix + "/collections"
        assert response.json()["base_url"].rstrip("/") == url_prefix
        links = response.json()["links"]
        for link in links:
            if link["rel"] in ["previous", "next"]:
                assert link["method"] == "GET"
            assert link["href"].startswith(url_prefix)
        assert {"next", "previous", "root", "self"} == {link["rel"] for link in links}
