import pytest
from fastapi import APIRouter, FastAPI
from starlette.requests import Request
from starlette.testclient import TestClient

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
