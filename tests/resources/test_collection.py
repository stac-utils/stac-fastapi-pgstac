from typing import Callable, Optional

import pystac
import pytest
from stac_pydantic import Collection


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
