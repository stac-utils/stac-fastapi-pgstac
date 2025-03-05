from typing import Callable, Optional

import pystac
import pytest
from stac_pydantic import Collection

from ..conftest import requires_pgstac_0_9_2


async def test_create_collection(app_client, load_test_data: Callable):
    in_json = load_test_data("test_collection.json")
    in_coll = Collection.model_validate(in_json)
    resp = await app_client.post(
        "/collections",
        json=in_json,
    )
    assert resp.status_code == 201

    post_coll = Collection.model_validate(resp.json())
    assert in_coll.model_dump(exclude={"links"}) == post_coll.model_dump(
        exclude={"links"}
    )
    resp = await app_client.get(f"/collections/{post_coll.id}")
    assert resp.status_code == 200
    get_coll = Collection.model_validate(resp.json())
    assert post_coll.model_dump(exclude={"links"}) == get_coll.model_dump(
        exclude={"links"}
    )

    post_coll = post_coll.model_dump(mode="json")
    get_coll = get_coll.model_dump(mode="json")
    post_self_link = next(
        (link for link in post_coll["links"] if link["rel"] == "self"), None
    )
    get_self_link = next(
        (link for link in get_coll["links"] if link["rel"] == "self"), None
    )
    assert post_self_link is not None and get_self_link is not None
    assert post_self_link["href"] == get_self_link["href"]


async def test_update_collection(app_client, load_test_data, load_test_collection):
    in_coll = load_test_collection
    in_coll["keywords"].append("newkeyword")

    resp = await app_client.put(f"/collections/{in_coll['id']}", json=in_coll)
    assert resp.status_code == 200
    put_coll = Collection.model_validate(resp.json())

    resp = await app_client.get(f"/collections/{in_coll['id']}")
    assert resp.status_code == 200

    get_coll = Collection.model_validate(resp.json())

    in_coll = Collection(**in_coll)
    assert in_coll.model_dump(exclude={"links"}) == get_coll.model_dump(exclude={"links"})
    assert "newkeyword" in get_coll.keywords

    get_coll = get_coll.model_dump(mode="json")
    put_coll = put_coll.model_dump(mode="json")
    put_self_link = next(
        (link for link in put_coll["links"] if link["rel"] == "self"), None
    )
    get_self_link = next(
        (link for link in get_coll["links"] if link["rel"] == "self"), None
    )
    assert put_self_link is not None and get_self_link is not None
    assert put_self_link["href"] == get_self_link["href"]


async def test_delete_collection(
    app_client, load_test_data: Callable, load_test_collection
):
    in_coll = load_test_collection

    resp = await app_client.delete(f"/collections/{in_coll['id']}")
    assert resp.status_code == 200

    resp = await app_client.get(f"/collections/{in_coll['id']}")
    assert resp.status_code == 404


async def test_create_collection_conflict(app_client, load_test_data: Callable):
    in_json = load_test_data("test_collection.json")
    Collection.model_validate(in_json)
    resp = await app_client.post(
        "/collections",
        json=in_json,
    )
    assert resp.status_code == 201
    Collection.model_validate(resp.json())
    resp = await app_client.post(
        "/collections",
        json=in_json,
    )
    assert resp.status_code == 409


async def test_delete_missing_collection(
    app_client,
):
    resp = await app_client.delete("/collections")
    assert resp.status_code == 405


async def test_update_new_collection(app_client, load_test_collection):
    in_coll = load_test_collection
    in_coll["id"] = "test-updatenew"

    resp = await app_client.put(f"/collections/{in_coll['id']}", json=in_coll)
    assert resp.status_code == 404


async def test_nocollections(
    app_client,
):
    resp = await app_client.get("/collections")
    assert resp.status_code == 200
    assert resp.json()["numberReturned"] == 0


async def test_returns_valid_collection(app_client, load_test_data):
    """Test updating a collection which already exists"""
    in_json = load_test_data("test_collection.json")
    resp = await app_client.post(
        "/collections",
        json=in_json,
    )
    assert resp.status_code == 201

    resp = await app_client.get(f"/collections/{in_json['id']}")
    assert resp.status_code == 200
    resp_json = resp.json()

    # Mock root to allow validation
    mock_root = pystac.Catalog(
        id="test", description="test desc", href="https://example.com"
    )
    collection = pystac.Collection.from_dict(
        resp_json, root=mock_root, preserve_dict=False
    )
    collection.validate()


async def test_returns_valid_links_in_collections(app_client, load_test_data):
    """Test links from listing collections"""
    in_json = load_test_data("test_collection.json")
    resp = await app_client.post(
        "/collections",
        json=in_json,
    )
    assert resp.status_code == 201

    # Get collection by ID
    resp = await app_client.get(f"/collections/{in_json['id']}")
    assert resp.status_code == 200
    resp_json = resp.json()

    # Mock root to allow validation
    mock_root = pystac.Catalog(
        id="test", description="test desc", href="https://example.com"
    )
    collection = pystac.Collection.from_dict(
        resp_json, root=mock_root, preserve_dict=False
    )
    assert collection.validate()

    # List collections
    resp = await app_client.get("/collections")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp.json()["numberReturned"]
    assert resp.json()["numberMatched"]

    collections = resp_json["collections"]
    # Find collection in list by ID
    single_coll = next(coll for coll in collections if coll["id"] == in_json["id"])
    is_coll_from_list_valid = False

    single_coll_mocked_link: Optional[pystac.Collection] = None
    if single_coll is not None:
        single_coll_mocked_link = pystac.Collection.from_dict(
            single_coll, root=mock_root, preserve_dict=False
        )
        is_coll_from_list_valid = single_coll_mocked_link.validate()

    assert is_coll_from_list_valid

    # Check links from the collection GET and list
    assert [
        i
        for i in collection.to_dict()["links"]
        if i not in single_coll_mocked_link.to_dict()["links"]
    ] == []


async def test_returns_license_link(app_client, load_test_collection):
    coll = load_test_collection

    resp = await app_client.get(f"/collections/{coll['id']}")
    assert resp.status_code == 200
    resp_json = resp.json()
    link_rel_types = [link["rel"] for link in resp_json["links"]]
    assert "license" in link_rel_types


@pytest.mark.asyncio
async def test_get_collection_forwarded_header(app_client, load_test_collection):
    coll = load_test_collection
    resp = await app_client.get(
        f"/collections/{coll['id']}",
        headers={"Forwarded": "proto=https;host=test:1234"},
    )
    for link in [
        link
        for link in resp.json()["links"]
        if link["rel"] in ["items", "parent", "root", "self"]
    ]:
        assert link["href"].startswith("https://test:1234/")


@pytest.mark.asyncio
async def test_get_collection_x_forwarded_headers(app_client, load_test_collection):
    coll = load_test_collection
    resp = await app_client.get(
        f"/collections/{coll['id']}",
        headers={
            "X-Forwarded-Port": "1234",
            "X-Forwarded-Proto": "https",
        },
    )
    for link in [
        link
        for link in resp.json()["links"]
        if link["rel"] in ["items", "parent", "root", "self"]
    ]:
        assert link["href"].startswith("https://test:1234/")


@pytest.mark.asyncio
async def test_get_collection_duplicate_forwarded_headers(
    app_client, load_test_collection
):
    coll = load_test_collection
    resp = await app_client.get(
        f"/collections/{coll['id']}",
        headers={
            "Forwarded": "proto=https;host=test:1234",
            "X-Forwarded-Port": "4321",
            "X-Forwarded-Proto": "http",
        },
    )
    for link in [
        link
        for link in resp.json()["links"]
        if link["rel"] in ["items", "parent", "root", "self"]
    ]:
        assert link["href"].startswith("https://test:1234/")


@pytest.mark.asyncio
async def test_get_collections_forwarded_header(app_client, load_test_collection):
    resp = await app_client.get(
        "/collections",
        headers={"Forwarded": "proto=https;host=test:1234"},
    )
    for link in resp.json()["links"]:
        assert link["href"].startswith("https://test:1234/")


@pytest.mark.asyncio
async def test_get_collections_queryables_links(app_client, load_test_collection):
    resp = await app_client.get(
        "/collections",
    )
    assert "Queryables" in [
        link.get("title") for link in resp.json()["collections"][0]["links"]
    ]

    collection_id = resp.json()["collections"][0]["id"]
    resp = await app_client.get(
        f"/collections/{collection_id}",
    )
    assert "Queryables" in [link.get("title") for link in resp.json()["links"]]


@pytest.mark.asyncio
async def test_get_collections_search(
    app_client, load_test_collection, load_test2_collection
):
    # this search should only return a single collection
    resp = await app_client.get(
        "/collections",
        params={"datetime": "2010-01-01T00:00:00Z/2010-01-02T00:00:00Z"},
    )
    assert len(resp.json()["collections"]) == 1
    assert resp.json()["collections"][0]["id"] == load_test2_collection.id

    # same with this one
    resp = await app_client.get(
        "/collections",
        params={"datetime": "2020-01-01T00:00:00Z/.."},
    )
    assert len(resp.json()["collections"]) == 1
    assert resp.json()["collections"][0]["id"] == load_test_collection["id"]

    # no params should return both collections
    resp = await app_client.get(
        "/collections",
    )
    assert len(resp.json()["collections"]) == 2


@requires_pgstac_0_9_2
@pytest.mark.asyncio
async def test_collection_search_freetext(
    app_client, load_test_collection, load_test2_collection
):
    # free-text
    resp = await app_client.get(
        "/collections",
        params={"q": "temperature"},
    )
    assert resp.json()["numberReturned"] == 1
    assert resp.json()["numberMatched"] == 1
    assert len(resp.json()["collections"]) == 1
    assert resp.json()["collections"][0]["id"] == load_test2_collection.id

    resp = await app_client.get(
        "/collections",
        params={"q": "nosuchthing"},
    )
    assert len(resp.json()["collections"]) == 0


@requires_pgstac_0_9_2
@pytest.mark.asyncio
async def test_all_collections_with_pagination(app_client, load_test_data):
    data = load_test_data("test_collection.json")
    collection_id = data["id"]
    for ii in range(0, 12):
        data["id"] = collection_id + f"_{ii}"
        resp = await app_client.post(
            "/collections",
            json=data,
        )
        assert resp.status_code == 201

    resp = await app_client.get("/collections")
    assert resp.json()["numberReturned"] == 10
    assert resp.json()["numberMatched"] == 12
    cols = resp.json()["collections"]
    assert len(cols) == 10
    links = resp.json()["links"]
    assert len(links) == 3
    assert {"root", "self", "next"} == {link["rel"] for link in links}

    resp = await app_client.get("/collections", params={"limit": 12})
    assert resp.json()["numberReturned"] == 12
    assert resp.json()["numberMatched"] == 12
    cols = resp.json()["collections"]
    assert len(cols) == 12
    links = resp.json()["links"]
    assert len(links) == 2
    assert {"root", "self"} == {link["rel"] for link in links}


@requires_pgstac_0_9_2
@pytest.mark.asyncio
async def test_all_collections_without_pagination(app_client_no_ext, load_test_data):
    data = load_test_data("test_collection.json")
    collection_id = data["id"]
    for ii in range(0, 12):
        data["id"] = collection_id + f"_{ii}"
        resp = await app_client_no_ext.post(
            "/collections",
            json=data,
        )
        assert resp.status_code == 201

    resp = await app_client_no_ext.get("/collections")
    assert resp.json()["numberReturned"] == 12
    assert resp.json()["numberMatched"] == 12
    cols = resp.json()["collections"]
    assert len(cols) == 12
    links = resp.json()["links"]
    assert len(links) == 2
    assert {"root", "self"} == {link["rel"] for link in links}


@requires_pgstac_0_9_2
@pytest.mark.asyncio
async def test_get_collections_search_pagination(
    app_client, load_test_collection, load_test2_collection
):
    resp = await app_client.get("/collections")
    assert resp.json()["numberReturned"] == 2
    assert resp.json()["numberMatched"] == 2
    cols = resp.json()["collections"]
    assert len(cols) == 2
    links = resp.json()["links"]
    assert len(links) == 2
    assert {"root", "self"} == {link["rel"] for link in links}

    ###################
    # limit should be positive
    resp = await app_client.get("/collections", params={"limit": 0})
    assert resp.status_code == 400

    ###################
    # limit=1, should have a `next` link
    resp = await app_client.get(
        "/collections",
        params={"limit": 1},
    )
    cols = resp.json()["collections"]
    links = resp.json()["links"]
    assert len(cols) == 1
    assert cols[0]["id"] == load_test_collection["id"]
    assert len(links) == 3
    assert {"root", "self", "next"} == {link["rel"] for link in links}
    next_link = list(filter(lambda link: link["rel"] == "next", links))[0]
    assert next_link["href"].endswith("?limit=1&offset=1")

    ###################
    # limit=2, there should not be a next link
    resp = await app_client.get(
        "/collections",
        params={"limit": 2},
    )
    cols = resp.json()["collections"]
    links = resp.json()["links"]
    assert len(cols) == 2
    assert cols[0]["id"] == load_test_collection["id"]
    assert cols[1]["id"] == load_test2_collection.id
    assert len(links) == 2
    assert {"root", "self"} == {link["rel"] for link in links}

    ###################
    # limit=3, there should not be a next/previous link
    resp = await app_client.get(
        "/collections",
        params={"limit": 3},
    )
    cols = resp.json()["collections"]
    links = resp.json()["links"]
    assert len(cols) == 2
    assert cols[0]["id"] == load_test_collection["id"]
    assert cols[1]["id"] == load_test2_collection.id
    assert len(links) == 2
    assert {"root", "self"} == {link["rel"] for link in links}

    ###################
    # offset=3, because there are 2 collections, we should not have `next` or `prev` links
    resp = await app_client.get(
        "/collections",
        params={"offset": 3},
    )
    cols = resp.json()["collections"]
    links = resp.json()["links"]
    assert len(cols) == 0
    assert len(links) == 2
    assert {"root", "self"} == {link["rel"] for link in links}

    ###################
    # offset=3,limit=1
    resp = await app_client.get(
        "/collections",
        params={"limit": 1, "offset": 3},
    )
    cols = resp.json()["collections"]
    links = resp.json()["links"]
    assert len(cols) == 0
    assert len(links) == 3
    assert {"root", "self", "previous"} == {link["rel"] for link in links}
    prev_link = list(filter(lambda link: link["rel"] == "previous", links))[0]
    assert prev_link["href"].endswith("?limit=1&offset=2")

    ###################
    # limit=2, offset=3, there should not be a next link
    resp = await app_client.get(
        "/collections",
        params={"limit": 2, "offset": 3},
    )
    cols = resp.json()["collections"]
    links = resp.json()["links"]
    assert len(cols) == 0
    assert len(links) == 3
    assert {"root", "self", "previous"} == {link["rel"] for link in links}
    prev_link = list(filter(lambda link: link["rel"] == "previous", links))[0]
    assert prev_link["href"].endswith("?limit=2&offset=1")

    ###################
    # offset=1,limit=1 should have a `previous` link
    resp = await app_client.get(
        "/collections",
        params={"offset": 1, "limit": 1},
    )
    cols = resp.json()["collections"]
    links = resp.json()["links"]
    assert len(cols) == 1
    assert cols[0]["id"] == load_test2_collection.id
    assert len(links) == 3
    assert {"root", "self", "previous"} == {link["rel"] for link in links}
    prev_link = list(filter(lambda link: link["rel"] == "previous", links))[0]
    assert "offset" in prev_link["href"]

    ###################
    # offset=0, should not have next/previous link
    resp = await app_client.get(
        "/collections",
        params={"offset": 0},
    )
    cols = resp.json()["collections"]
    links = resp.json()["links"]
    assert len(cols) == 2
    assert len(links) == 2
    assert {"root", "self"} == {link["rel"] for link in links}


@requires_pgstac_0_9_2
@pytest.mark.xfail(strict=False)
@pytest.mark.asyncio
async def test_get_collections_search_offset_1(
    app_client, load_test_collection, load_test2_collection
):
    # BUG: pgstac doesn't return a `prev` link when limit is not set
    # offset=1, should have a `previous` link
    resp = await app_client.get(
        "/collections",
        params={"offset": 1},
    )
    cols = resp.json()["collections"]
    links = resp.json()["links"]
    assert len(cols) == 1
    assert cols[0]["id"] == load_test2_collection.id
    assert len(links) == 3
    assert {"root", "self", "previous"} == {link["rel"] for link in links}
    prev_link = list(filter(lambda link: link["rel"] == "previous", links))[0]
    # offset=0 should not be in the previous link (because it's useless)
    assert "offset" not in prev_link["href"]
