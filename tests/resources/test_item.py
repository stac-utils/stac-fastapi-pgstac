import json
import random
import uuid
from datetime import timedelta
from http.client import HTTP_PORT
from string import ascii_letters
from typing import Callable
from urllib.parse import parse_qs, urljoin, urlparse

import pystac
import pytest
from httpx import AsyncClient
from pystac.utils import datetime_to_str
from shapely.geometry import Polygon
from stac_fastapi.types.rfc3339 import rfc3339_str_to_datetime
from stac_pydantic import Collection, Item
from starlette.requests import Request

from stac_fastapi.pgstac.models.links import CollectionLinks


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


async def test_update_collection(app_client, load_test_data, load_test_collection):
    in_coll = load_test_collection
    in_coll = load_test_data("test_collection.json")
    in_coll["keywords"].append("newkeyword")

    resp = await app_client.put(f"/collections/{in_coll['id']}", json=in_coll)
    assert resp.status_code == 200

    resp = await app_client.get(f"/collections/{in_coll['id']}")
    assert resp.status_code == 200

    get_coll = Collection.model_validate(resp.json())

    in_coll = Collection(**in_coll)
    assert in_coll.model_dump(exclude={"links"}) == get_coll.model_dump(exclude={"links"})
    assert "newkeyword" in get_coll.keywords


async def test_delete_collection(
    app_client, load_test_data: Callable, load_test_collection
):
    in_coll = load_test_collection

    resp = await app_client.delete(f"/collections/{in_coll['id']}")
    assert resp.status_code == 200

    resp = await app_client.get(f"/collections/{in_coll['id']}")
    assert resp.status_code == 404


async def test_create_item(app_client, load_test_data: Callable, load_test_collection):
    coll = load_test_collection

    in_json = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{coll['id']}/items",
        json=in_json,
    )
    assert resp.status_code == 201

    in_item = Item.model_validate(in_json)
    post_item = Item.model_validate(resp.json())
    assert in_item.model_dump(exclude={"links"}) == post_item.model_dump(
        exclude={"links"}
    )

    resp = await app_client.get(f"/collections/{coll['id']}/items/{post_item.id}")

    assert resp.status_code == 200
    get_item = Item.model_validate(resp.json())
    assert in_item.model_dump(exclude={"links"}) == get_item.model_dump(exclude={"links"})

    get_item = get_item.model_dump(mode="json")
    post_item = post_item.model_dump(mode="json")
    post_self_link = next(
        (link for link in post_item["links"] if link["rel"] == "self"), None
    )
    get_self_link = next(
        (link for link in get_item["links"] if link["rel"] == "self"), None
    )
    assert post_self_link is not None and get_self_link is not None
    assert post_self_link["href"] == get_self_link["href"]


async def test_create_item_mismatched_collection_id(
    app_client, load_test_data: Callable, load_test_collection
):
    # If the collection_id path parameter and the Item's "collection" property do not match, a 400 response should
    # be returned.
    coll = load_test_collection

    in_json = load_test_data("test_item.json")
    in_json["collection"] = random.choice(ascii_letters)
    assert in_json["collection"] != coll["id"]

    resp = await app_client.post(
        f"/collections/{coll['id']}/items",
        json=in_json,
    )
    assert resp.status_code == 400


async def test_fetches_valid_item(
    app_client, load_test_data: Callable, load_test_collection
):
    coll = load_test_collection

    in_json = load_test_data("test_item.json")

    resp = await app_client.post(
        f"/collections/{coll['id']}/items",
        json=in_json,
    )
    assert resp.status_code == 201

    in_item = Item.model_validate(in_json)
    post_item = Item.model_validate(resp.json())
    assert in_item.model_dump(exclude={"links"}) == post_item.model_dump(
        exclude={"links"}
    )

    resp = await app_client.get(f"/collections/{coll['id']}/items/{post_item.id}")

    assert resp.status_code == 200
    item_dict = resp.json()
    # Mock root to allow validation
    mock_root = pystac.Catalog(
        id="test", description="test desc", href="https://example.com"
    )
    item = pystac.Item.from_dict(
        item_dict, preserve_dict=False, root=mock_root, migrate=False
    )
    item.validate()


async def test_update_item(
    app_client, load_test_data: Callable, load_test_collection, load_test_item
):
    coll = load_test_collection
    item = load_test_item

    item["properties"]["description"] = "Update Test"

    resp = await app_client.put(
        f"/collections/{coll['id']}/items/{item['id']}", json=item
    )
    assert resp.status_code == 200
    put_item = Item.model_validate(resp.json())

    resp = await app_client.get(f"/collections/{coll['id']}/items/{item['id']}")
    assert resp.status_code == 200

    get_item = Item.model_validate(resp.json())
    item = Item(**item)
    assert item.model_dump(exclude={"links"}) == get_item.model_dump(exclude={"links"})
    assert get_item.properties.description == "Update Test"

    put_item = put_item.model_dump(mode="json")
    get_item = get_item.model_dump(mode="json")
    post_self_link = next(
        (link for link in put_item["links"] if link["rel"] == "self"), None
    )
    get_self_link = next(
        (link for link in get_item["links"] if link["rel"] == "self"), None
    )
    assert post_self_link is not None and get_self_link is not None
    assert post_self_link["href"] == get_self_link["href"]


async def test_patch_item_partialitem(
    app_client,
    load_test_collection: Collection,
    load_test_item: Item,
):
    """Test patching an Item with a PartialCollection."""
    item_id = load_test_item["id"]
    collection_id = load_test_item["collection"]
    assert collection_id == load_test_collection["id"]
    partial = {
        "id": item_id,
        "collection": collection_id,
        "properties": {"gsd": 10},
    }

    resp = await app_client.patch(
        f"/collections/{collection_id}/items/{item_id}", json=partial
    )
    assert resp.status_code == 200

    resp = await app_client.get(f"/collections/{collection_id}/items/{item_id}")
    assert resp.status_code == 200

    get_item_json = resp.json()
    Item.model_validate(get_item_json)

    assert get_item_json["properties"]["gsd"] == 10


async def test_patch_item_operations(
    app_client,
    load_test_collection: Collection,
    load_test_item: Item,
):
    """Test patching an Item with PatchOperations ."""

    item_id = load_test_item["id"]
    collection_id = load_test_item["collection"]
    assert collection_id == load_test_collection["id"]
    operations = [{"op": "replace", "path": "/properties/gsd", "value": 20}]

    resp = await app_client.patch(
        f"/collections/{collection_id}/items/{item_id}", json=operations
    )
    assert resp.status_code == 200

    resp = await app_client.get(f"/collections/{collection_id}/items/{item_id}")
    assert resp.status_code == 200

    get_item_json = resp.json()
    Item.model_validate(get_item_json)

    assert get_item_json["properties"]["gsd"] == 20


async def test_update_item_mismatched_collection_id(
    app_client, load_test_data: Callable, load_test_collection, load_test_item
) -> None:
    coll = load_test_collection

    in_json = load_test_data("test_item.json")

    in_json["collection"] = random.choice(ascii_letters)
    assert in_json["collection"] != coll["id"]

    item_id = in_json["id"]

    resp = await app_client.put(
        f"/collections/{coll['id']}/items/{item_id}",
        json=in_json,
    )
    assert resp.status_code == 400


async def test_delete_item(
    app_client, load_test_data: Callable, load_test_collection, load_test_item
):
    coll = load_test_collection
    item = load_test_item

    resp = await app_client.delete(f"/collections/{coll['id']}/items/{item['id']}")
    assert resp.status_code == 200

    resp = await app_client.get(f"/collections/{coll['id']}/items/{item['id']}")
    assert resp.status_code == 404


async def test_get_collection_items(
    app_client, load_test_collection, load_test_item, load_test_data
):
    coll = load_test_collection
    item = load_test_data("test_item.json")

    for _ in range(4):
        item["id"] = str(uuid.uuid4())
        resp = await app_client.post(
            f"/collections/{coll['id']}/items",
            json=item,
        )
        assert resp.status_code == 201

    resp = await app_client.get(
        f"/collections/{coll['id']}/items",
    )
    assert resp.status_code == 200
    fc = resp.json()
    assert "features" in fc
    assert len(fc["features"]) == 5


async def test_create_item_conflict(
    app_client, load_test_data: Callable, load_test_collection
):
    coll = load_test_collection
    in_json = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{coll['id']}/items",
        json=in_json,
    )
    assert resp.status_code == 201

    resp = await app_client.post(
        f"/collections/{coll['id']}/items",
        json=in_json,
    )
    assert resp.status_code == 409


async def test_delete_missing_item(
    app_client, load_test_data: Callable, load_test_collection, load_test_item
):
    coll = load_test_collection
    item = load_test_item

    resp = await app_client.delete(f"/collections/{coll['id']}/items/{item['id']}")
    assert resp.status_code == 200

    resp = await app_client.delete(f"/collections/{coll['id']}/items/{item['id']}")
    assert resp.status_code == 404


async def test_create_item_missing_collection(
    app_client, load_test_data: Callable, load_test_collection
):
    coll = load_test_collection
    item = load_test_data("test_item.json")
    item["collection"] = None

    resp = await app_client.post(f"/collections/{coll['id']}/items", json=item)
    assert resp.status_code == 201

    post_item = resp.json()
    assert post_item["collection"] == coll["id"]


async def test_update_new_item(
    app_client, load_test_data: Callable, load_test_collection
):
    coll = load_test_collection

    new_item = load_test_data("test_item.json")
    new_item["id"] = "test-updatenewitem"

    resp = await app_client.put(
        f"/collections/{coll['id']}/items/{new_item['id']}", json=new_item
    )
    assert resp.status_code == 404


async def test_update_item_missing_collection(
    app_client, load_test_data: Callable, load_test_collection, load_test_item
):
    coll = load_test_collection
    item = load_test_item
    item["collection"] = None

    resp = await app_client.put(
        f"/collections/{coll['id']}/items/{item['id']}", json=item
    )
    assert resp.status_code == 200

    put_item = resp.json()
    assert put_item["collection"] == coll["id"]


async def test_pagination(app_client, load_test_data, load_test_collection):
    """Test item collection pagination (paging extension)"""
    coll = load_test_collection
    item_count = 21

    for idx in range(1, item_count):
        item = load_test_data("test_item.json")
        item["id"] = item["id"] + str(idx)
        item["properties"]["datetime"] = f"2020-01-{idx:02d}T00:00:00Z"
        resp = await app_client.post(f"/collections/{coll['id']}/items", json=item)
        assert resp.status_code == 201

    resp = await app_client.get(f"/collections/{coll['id']}/items", params={"limit": 3})
    assert resp.status_code == 200
    first_page = resp.json()
    assert len(first_page["features"]) == 3

    nextlink = [
        link["href"] for link in first_page["links"] if link["rel"] == "next"
    ].pop()

    assert nextlink is not None

    assert [f["id"] for f in first_page["features"]] == [
        "test-item20",
        "test-item19",
        "test-item18",
    ]

    resp = await app_client.get(nextlink)
    assert resp.status_code == 200
    second_page = resp.json()
    assert len(first_page["features"]) == 3

    nextlink = [
        link["href"] for link in second_page["links"] if link["rel"] == "next"
    ].pop()

    assert nextlink is not None

    prevlink = [
        link["href"] for link in second_page["links"] if link["rel"] == "previous"
    ].pop()

    assert prevlink is not None

    assert [f["id"] for f in second_page["features"]] == [
        "test-item17",
        "test-item16",
        "test-item15",
    ]

    resp = await app_client.get(prevlink)
    assert resp.status_code == 200
    back_page = resp.json()
    assert len(back_page["features"]) == 3
    assert [f["id"] for f in back_page["features"]] == [
        "test-item20",
        "test-item19",
        "test-item18",
    ]


async def test_item_search_by_id_post(app_client, load_test_data, load_test_collection):
    """Test POST search by item id (core)"""
    ids = ["test1", "test2", "test3"]
    for id in ids:
        test_item = load_test_data("test_item.json")
        test_item["id"] = id
        resp = await app_client.post(
            f"/collections/{test_item['collection']}/items", json=test_item
        )
        assert resp.status_code == 201

    params = {"collections": [test_item["collection"]], "ids": ids}
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == len(ids)
    assert {feat["id"] for feat in resp_json["features"]} == set(ids)


async def test_item_search_by_id_no_results_post(
    app_client, load_test_data, load_test_collection
):
    """Test POST search by item id (core) when there are no results"""
    test_item = load_test_data("test_item.json")

    search_ids = ["nonexistent_id"]

    params = {"collections": [test_item["collection"]], "ids": search_ids}
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 0


async def test_item_search_spatial_query_post(
    app_client, load_test_data, load_test_collection
):
    """Test POST search with spatial query (core)"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    # Add second item with a different datetime.
    second_test_item = load_test_data("test_item2.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=second_test_item
    )
    assert resp.status_code == 201

    params = {
        "collections": [test_item["collection"]],
        "intersects": test_item["geometry"],
    }
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1
    assert resp_json["features"][0]["id"] == test_item["id"]


async def test_item_search_temporal_query_post(
    app_client, load_test_data, load_test_collection
):
    """Test POST search with single-tailed spatio-temporal query (core)"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    # Add second item with a different datetime.
    second_test_item = load_test_data("test_item2.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=second_test_item
    )
    assert resp.status_code == 201

    item_date = rfc3339_str_to_datetime(test_item["properties"]["datetime"])

    params = {
        "collections": [test_item["collection"]],
        "intersects": test_item["geometry"],
        "datetime": datetime_to_str(item_date),
    }

    resp = await app_client.post("/search", json=params)
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1
    assert resp_json["features"][0]["id"] == test_item["id"]


async def test_item_search_temporal_window_post(
    app_client, load_test_data, load_test_collection
):
    """Test POST search with two-tailed spatio-temporal query (core)"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    # Add second item with a different datetime.
    second_test_item = load_test_data("test_item2.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=second_test_item
    )
    assert resp.status_code == 201

    item_date = rfc3339_str_to_datetime(test_item["properties"]["datetime"])
    item_date_before = item_date - timedelta(seconds=1)
    item_date_after = item_date + timedelta(seconds=1)

    params = {
        "collections": [test_item["collection"]],
        "datetime": f"{datetime_to_str(item_date_before)}/{datetime_to_str(item_date_after)}",
    }

    resp = await app_client.post("/search", json=params)
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1
    assert resp_json["features"][0]["id"] == test_item["id"]


async def test_item_search_temporal_open_window(
    app_client, load_test_data, load_test_collection
):
    for dt in ["/", "../..", "../", "/.."]:
        resp = await app_client.post("/search", json={"datetime": dt})
        assert resp.status_code == 400


async def test_item_search_sort_post(app_client, load_test_data, load_test_collection):
    """Test POST search with sorting (sort extension)"""
    first_item = load_test_data("test_item.json")
    item_date = rfc3339_str_to_datetime(first_item["properties"]["datetime"])
    resp = await app_client.post(
        f"/collections/{first_item['collection']}/items", json=first_item
    )
    assert resp.status_code == 201

    second_item = load_test_data("test_item.json")
    second_item["id"] = "another-item"
    another_item_date = item_date - timedelta(days=1)
    second_item["properties"]["datetime"] = datetime_to_str(another_item_date)
    resp = await app_client.post(
        f"/collections/{second_item['collection']}/items", json=second_item
    )
    assert resp.status_code == 201

    params = {
        "collections": [first_item["collection"]],
        "sortby": [{"field": "datetime", "direction": "desc"}],
    }
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == first_item["id"]
    assert resp_json["features"][1]["id"] == second_item["id"]


async def test_item_search_by_id_get(app_client, load_test_data, load_test_collection):
    """Test GET search by item id (core)"""
    ids = ["test1", "test2", "test3"]
    for id in ids:
        test_item = load_test_data("test_item.json")
        test_item["id"] = id
        resp = await app_client.post(
            f"/collections/{test_item['collection']}/items", json=test_item
        )
        assert resp.status_code == 201

    params = {"collections": test_item["collection"], "ids": ",".join(ids)}
    resp = await app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == len(ids)
    assert {feat["id"] for feat in resp_json["features"]} == set(ids)


async def test_item_search_bbox_get(app_client, load_test_data, load_test_collection):
    """Test GET search with spatial query (core)"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    # Add second item with a different datetime.
    second_test_item = load_test_data("test_item2.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=second_test_item
    )
    assert resp.status_code == 201

    params = {
        "collections": test_item["collection"],
        "bbox": ",".join([str(coord) for coord in test_item["bbox"]]),
    }
    resp = await app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1
    assert resp_json["features"][0]["id"] == test_item["id"]


async def test_item_search_get_without_collections(
    app_client, load_test_data, load_test_collection
):
    """Test GET search without specifying collections"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    # Add second item with a different datetime.
    second_test_item = load_test_data("test_item2.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=second_test_item
    )
    assert resp.status_code == 201

    params = {
        "bbox": ",".join([str(coord) for coord in test_item["bbox"]]),
    }
    resp = await app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1
    assert resp_json["features"][0]["id"] == test_item["id"]


async def test_item_search_temporal_window_get(
    app_client, load_test_data, load_test_collection
):
    """Test GET search with spatio-temporal query (core)"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    # Add second item with a different datetime.
    second_test_item = load_test_data("test_item2.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=second_test_item
    )
    assert resp.status_code == 201

    item_date = rfc3339_str_to_datetime(test_item["properties"]["datetime"])
    item_date_before = item_date - timedelta(seconds=1)
    item_date_after = item_date + timedelta(seconds=1)

    params = {
        "collections": test_item["collection"],
        "datetime": f"{datetime_to_str(item_date_before)}/{datetime_to_str(item_date_after)}",
    }
    resp = await app_client.get("/search", params=params)
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1
    assert resp_json["features"][0]["id"] == test_item["id"]


async def test_item_search_sort_get(app_client, load_test_data, load_test_collection):
    """Test GET search with sorting (sort extension)"""
    first_item = load_test_data("test_item.json")
    item_date = rfc3339_str_to_datetime(first_item["properties"]["datetime"])
    resp = await app_client.post(
        f"/collections/{first_item['collection']}/items", json=first_item
    )
    assert resp.status_code == 201

    second_item = load_test_data("test_item.json")
    second_item["id"] = "another-item"
    another_item_date = item_date - timedelta(days=1)
    second_item["properties"]["datetime"] = datetime_to_str(another_item_date)
    resp = await app_client.post(
        f"/collections/{second_item['collection']}/items", json=second_item
    )
    assert resp.status_code == 201
    params = {"collections": [first_item["collection"]], "sortby": "-datetime"}
    resp = await app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == first_item["id"]
    assert resp_json["features"][1]["id"] == second_item["id"]


async def test_item_search_post_without_collection(
    app_client, load_test_data, load_test_collection
):
    """Test POST search without specifying a collection"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    second_test_item = load_test_data("test_item2.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=second_test_item
    )
    assert resp.status_code == 201

    params = {
        "bbox": test_item["bbox"],
    }
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == test_item["id"]


async def test_item_search_properties_jsonb(
    app_client, load_test_data, load_test_collection
):
    """Test POST search with JSONB query (query extension)"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    second_test_item = load_test_data("test_item2.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=second_test_item
    )
    assert resp.status_code == 201

    # EPSG is a JSONB key
    params = {"query": {"proj:epsg": {"gt": test_item["properties"]["proj:epsg"] - 1}}}
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


async def test_item_search_properties_field(
    app_client, load_test_data, load_test_collection
):
    """Test POST search indexed field with query (query extension)"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    second_test_item = load_test_data("test_item2.json")
    second_test_item["properties"]["eo:cloud_cover"] = 5
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=second_test_item
    )
    assert resp.status_code == 201

    params = {"query": {"eo:cloud_cover": {"eq": 0}}}
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


async def test_item_search_get_query_extension(
    app_client, load_test_data, load_test_collection
):
    """Test GET search with JSONB query (query extension)"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    second_test_item = load_test_data("test_item2.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=second_test_item
    )
    assert resp.status_code == 201

    # EPSG is a JSONB key
    params = {
        "collections": [test_item["collection"]],
        "query": json.dumps(
            {"proj:epsg": {"gt": test_item["properties"]["proj:epsg"] + 1}}
        ),
    }
    resp = await app_client.get("/search", params=params)
    # No items found should still return a 200 but with an empty list of features
    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 0

    params["query"] = json.dumps(
        {"proj:epsg": {"eq": test_item["properties"]["proj:epsg"]}}
    )
    resp = await app_client.get("/search", params=params)
    resp_json = resp.json()
    assert len(resp.json()["features"]) == 1
    assert (
        resp_json["features"][0]["properties"]["proj:epsg"]
        == test_item["properties"]["proj:epsg"]
    )


async def test_item_search_post_filter_extension_cql(
    app_client, load_test_data, load_test_collection
):
    """Test POST search with JSONB query (cql json filter extension)"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    second_test_item = load_test_data("test_item2.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=second_test_item
    )
    assert resp.status_code == 201

    # EPSG is a JSONB key
    params = {
        "collections": [test_item["collection"]],
        "filter": {
            "gt": [
                {"property": "proj:epsg"},
                test_item["properties"]["proj:epsg"] + 1,
            ]
        },
    }
    resp = await app_client.post("/search", json=params)
    resp_json = resp.json()

    assert resp.status_code == 200
    assert len(resp_json.get("features")) == 0

    params = {
        "collections": [test_item["collection"]],
        "filter": {
            "eq": [
                {"property": "proj:epsg"},
                test_item["properties"]["proj:epsg"],
            ]
        },
    }
    resp = await app_client.post("/search", json=params)
    resp_json = resp.json()
    assert len(resp.json()["features"]) == 1
    assert (
        resp_json["features"][0]["properties"]["proj:epsg"]
        == test_item["properties"]["proj:epsg"]
    )


async def test_item_search_post_filter_extension_cql2(
    app_client, load_test_data, load_test_collection
):
    """Test POST search with JSONB query (cql2 json filter extension)"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    second_test_item = load_test_data("test_item2.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=second_test_item
    )
    assert resp.status_code == 201

    # make sure we have 2 items
    resp = await app_client.post("/search", json={})
    resp_json = resp.json()
    assert resp.status_code == 200
    assert len(resp_json.get("features")) == 2

    # EPSG is a JSONB key
    params = {
        "collections": [test_item["collection"]],
        "filter-lang": "cql2-json",
        "filter": {
            "op": "gt",
            "args": [
                {"property": "proj:epsg"},
                test_item["properties"]["proj:epsg"] + 1,
            ],
        },
    }
    resp = await app_client.post("/search", json=params)
    resp_json = resp.json()

    assert resp.status_code == 200
    assert len(resp_json.get("features")) == 0

    params = {
        "collections": [test_item["collection"]],
        "filter-lang": "cql2-json",
        "filter": {
            "op": "eq",
            "args": [
                {"property": "proj:epsg"},
                test_item["properties"]["proj:epsg"],
            ],
        },
    }
    resp = await app_client.post("/search", json=params)
    resp_json = resp.json()
    assert len(resp.json()["features"]) == 1
    assert (
        resp_json["features"][0]["properties"]["proj:epsg"]
        == test_item["properties"]["proj:epsg"]
    )

    # Test IN operator
    params = {
        "collections": [test_item["collection"]],
        "filter-lang": "cql2-json",
        "filter": {
            "op": "in",
            "args": [
                {"property": "proj:epsg"},
                [test_item["properties"]["proj:epsg"]],
            ],
        },
    }
    resp = await app_client.post("/search", json=params)
    resp_json = resp.json()
    assert resp.status_code == 200
    assert len(resp_json.get("features")) == 1

    params = {
        "collections": [test_item["collection"]],
        "filter-lang": "cql2-json",
        "filter": {
            "op": "in",
            "args": [
                {"property": "proj:epsg"},
                [test_item["properties"]["proj:epsg"] + 1],
            ],
        },
    }
    resp = await app_client.post("/search", json=params)
    resp_json = resp.json()
    assert resp.status_code == 200
    assert len(resp_json.get("features")) == 0


async def test_item_search_post_filter_extension_cql2_with_query_fails(
    app_client, load_test_data, load_test_collection
):
    """Test POST search with JSONB query (cql2 json filter extension)"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    second_test_item = load_test_data("test_item2.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=second_test_item
    )
    assert resp.status_code == 201

    # Cannot use `query` and `filter`
    params = {
        "collections": [test_item["collection"]],
        "filter-lang": "cql2-json",
        "filter": {
            "op": "gt",
            "args": [
                {"property": "proj:epsg"},
                test_item["properties"]["proj:epsg"] + 1,
            ],
        },
        "query": {"eo:cloud_cover": {"eq": 0}},
    }
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 400


async def test_get_missing_item_collection(app_client):
    """Test reading a collection which does not exist"""
    resp = await app_client.get("/collections/invalid-collection/items")
    assert resp.status_code == 404


async def test_get_item_from_missing_item_collection(app_client):
    """Test reading an item from a collection which does not exist"""
    resp = await app_client.get("/collections/invalid-collection/items/some-item")
    assert resp.status_code == 404


async def test_pagination_item_collection(
    app_client, load_test_data, load_test_collection
):
    """Test item collection pagination links (paging extension)"""
    test_item = load_test_data("test_item.json")
    ids = []

    # Ingest 5 items
    for _ in range(5):
        uid = str(uuid.uuid4())
        test_item["id"] = uid
        resp = await app_client.post(
            f"/collections/{test_item['collection']}/items", json=test_item
        )
        assert resp.status_code == 201
        ids.append(uid)

    # Paginate through all 5 items with a limit of 1 (expecting 5 requests)
    page = await app_client.get(
        f"/collections/{test_item['collection']}/items", params={"limit": 1}
    )
    idx = 0
    item_ids = []
    while True:
        idx += 1
        page_data = page.json()
        item_ids.append(page_data["features"][0]["id"])
        nextlink = [link["href"] for link in page_data["links"] if link["rel"] == "next"]
        if len(nextlink) < 1:
            break

        page = await app_client.get(nextlink.pop())

        assert idx < 10

    # Our limit is 1 so we expect len(ids) number of requests before we run out of pages
    assert idx == len(ids)

    # Confirm we have paginated through all items
    assert not set(item_ids) - set(ids)


async def test_pagination_post(app_client, load_test_data, load_test_collection):
    """Test POST pagination (paging extension)"""
    test_item = load_test_data("test_item.json")
    ids = []

    # Ingest 5 items
    for _ in range(5):
        uid = str(uuid.uuid4())
        test_item["id"] = uid
        resp = await app_client.post(
            f"/collections/{test_item['collection']}/items", json=test_item
        )
        assert resp.status_code == 201
        ids.append(uid)

    # Paginate through all 5 items with a limit of 1 (expecting 5 requests)
    request_body = {
        "filter-lang": "cql2-json",
        "filter": {"op": "in", "args": [{"property": "id"}, ids]},
        "limit": 1,
    }
    page = await app_client.post("/search", json=request_body)
    idx = 0
    item_ids = []
    while True:
        idx += 1
        page_data = page.json()
        item_ids.append(page_data["features"][0]["id"])
        next_link = list(filter(lambda link: link["rel"] == "next", page_data["links"]))
        if not next_link:
            break

        # Merge request bodies
        request_body.update(next_link[0]["body"])
        page = await app_client.post("/search", json=request_body)

        assert idx < 10

    # Our limit is 1 so we expect len(ids) number of requests before we run out of pages
    assert idx == len(ids)

    # Confirm we have paginated through all items
    assert not set(item_ids) - set(ids)


async def test_pagination_token_idempotent(
    app_client, load_test_data, load_test_collection
):
    """Test that pagination tokens are idempotent (paging extension)"""
    test_item = load_test_data("test_item.json")
    ids = []

    # Ingest 5 items
    for _ in range(5):
        uid = str(uuid.uuid4())
        test_item["id"] = uid
        resp = await app_client.post(
            f"/collections/{test_item['collection']}/items", json=test_item
        )
        assert resp.status_code == 201
        ids.append(uid)

    page = await app_client.post(
        "/search",
        json={
            "filter-lang": "cql2-json",
            "filter": {"op": "in", "args": [{"property": "id"}, ids]},
            "limit": 3,
        },
    )
    page_data = page.json()
    next_link = list(filter(lambda link: link["rel"] == "next", page_data["links"]))

    # Confirm token is idempotent
    resp1 = await app_client.get(
        "/search", params=parse_qs(urlparse(next_link[0]["href"]).query)
    )
    resp2 = await app_client.get(
        "/search", params=parse_qs(urlparse(next_link[0]["href"]).query)
    )
    resp1_data = resp1.json()
    resp2_data = resp2.json()

    # Two different requests with the same pagination token should return the same items
    assert [item["id"] for item in resp1_data["features"]] == [
        item["id"] for item in resp2_data["features"]
    ]


async def test_field_extension_get(app_client, load_test_data, load_test_collection):
    """Test GET search with included fields (fields extension)"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    params = {"fields": "+properties.proj:epsg,+properties.gsd,+collection"}
    resp = await app_client.get(
        f"/collections/{test_item['collection']}/items", params=params
    )
    feat_properties = resp.json()["features"][0]["properties"]
    assert not set(feat_properties) - {"proj:epsg", "gsd", "datetime"}

    params = {"fields": "+properties.proj:epsg,+properties.gsd,+collection"}
    resp = await app_client.get("/search", params=params)
    feat_properties = resp.json()["features"][0]["properties"]
    assert not set(feat_properties) - {"proj:epsg", "gsd", "datetime"}


async def test_field_extension_post(app_client, load_test_data, load_test_collection):
    """Test POST search with included and excluded fields (fields extension)"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    body = {
        "fields": {
            "exclude": ["assets.B1"],
            "include": [
                "properties.eo:cloud_cover",
                "properties.orientation",
                "assets",
                "collection",
            ],
        }
    }

    resp = await app_client.post("/search", json=body)
    resp_json = resp.json()
    assert "B1" not in resp_json["features"][0]["assets"].keys()
    assert not set(resp_json["features"][0]["properties"]) - {
        "orientation",
        "eo:cloud_cover",
        "datetime",
    }


async def test_field_extension_exclude_and_include(
    app_client, load_test_data, load_test_collection
):
    """Test POST search including/excluding same field (fields extension)"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    body = {
        "fields": {
            "exclude": ["properties.eo:cloud_cover"],
            "include": ["properties.eo:cloud_cover", "collection"],
        }
    }

    resp = await app_client.post("/search", json=body)
    resp_json = resp.json()
    assert "properties" not in resp_json["features"][0]


async def test_field_extension_exclude_default_includes(
    app_client, load_test_data, load_test_collection
):
    """Test POST search excluding a forbidden field (fields extension)"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    body = {"fields": {"exclude": ["geometry"]}}

    resp = await app_client.post("/search", json=body)
    resp_json = resp.json()
    assert "geometry" not in resp_json["features"][0]


async def test_field_extension_include_multiple_subkeys(
    app_client, load_test_item, load_test_collection
):
    """Test that multiple subkeys of an object field are included"""
    body = {"fields": {"include": ["properties.width", "properties.height"]}}

    resp = await app_client.post("/search", json=body)
    assert resp.status_code == 200
    resp_json = resp.json()

    resp_prop_keys = resp_json["features"][0]["properties"].keys()
    assert set(resp_prop_keys) == {"width", "height"}


async def test_field_extension_include_multiple_deeply_nested_subkeys(
    app_client, load_test_item, load_test_collection
):
    """Test that multiple deeply nested subkeys of an object field are included"""
    body = {"fields": {"include": ["assets.ANG.type", "assets.ANG.href"]}}

    resp = await app_client.post("/search", json=body)
    assert resp.status_code == 200
    resp_json = resp.json()

    resp_assets = resp_json["features"][0]["assets"]
    assert set(resp_assets.keys()) == {"ANG"}
    assert set(resp_assets["ANG"].keys()) == {"type", "href"}


async def test_field_extension_exclude_multiple_deeply_nested_subkeys(
    app_client, load_test_item, load_test_collection
):
    """Test that multiple deeply nested subkeys of an object field are excluded"""
    body = {"fields": {"exclude": ["assets.ANG.type", "assets.ANG.href"]}}

    resp = await app_client.post("/search", json=body)
    assert resp.status_code == 200
    resp_json = resp.json()

    resp_assets = resp_json["features"][0]["assets"]
    assert len(resp_assets.keys()) > 0
    assert "type" not in resp_assets["ANG"]
    assert "href" not in resp_assets["ANG"]


async def test_field_extension_exclude_deeply_nested_included_subkeys(
    app_client, load_test_item, load_test_collection
):
    """Test that deeply nested keys of a nested object that was included are excluded"""
    body = {
        "fields": {
            "include": ["assets.ANG.type", "assets.ANG.href"],
            "exclude": ["assets.ANG.href"],
        }
    }

    resp = await app_client.post("/search", json=body)
    assert resp.status_code == 200
    resp_json = resp.json()

    resp_assets = resp_json["features"][0]["assets"]
    assert "type" in resp_assets["ANG"]
    assert "href" not in resp_assets["ANG"]


async def test_field_extension_exclude_links(
    app_client, load_test_item, load_test_collection
):
    """Links have special injection behavior, ensure they can be excluded with the fields extension"""
    body = {"fields": {"exclude": ["links"]}}

    resp = await app_client.post("/search", json=body)
    assert resp.status_code == 200
    resp_json = resp.json()

    assert "links" not in resp_json["features"][0]


async def test_field_extension_include_only_non_existant_field(
    app_client, load_test_item, load_test_collection
):
    """Including only a non-existant field should return the full item"""
    body = {"fields": {"include": ["non_existant_field"]}}

    resp = await app_client.post("/search", json=body)
    assert resp.status_code == 200
    resp_json = resp.json()

    assert list(resp_json["features"][0].keys()) == ["id", "collection", "links"]


async def test_search_intersects_and_bbox(app_client):
    """Test POST search intersects and bbox are mutually exclusive (core)"""
    bbox = [-118, 34, -117, 35]
    geoj = Polygon.from_bounds(*bbox).__geo_interface__
    params = {"bbox": bbox, "intersects": geoj}
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 400


async def test_get_missing_item(app_client, load_test_data):
    """Test read item which does not exist (transactions extension)"""
    test_coll = load_test_data("test_collection.json")
    resp = await app_client.get(f"/collections/{test_coll['id']}/items/invalid-item")
    assert resp.status_code == 404


async def test_relative_link_construction(app):
    req = Request(
        scope={
            "type": "http",
            "scheme": "http",
            "method": "PUT",
            "root_path": "/stac",  # root_path should not have proto, domain, or port
            "path": "/",
            "raw_path": b"/tab/abc",
            "query_string": b"",
            "headers": {},
            "app": app,
            "server": ("test", HTTP_PORT),
        }
    )
    links = CollectionLinks(collection_id="naip", request=req)
    assert links.link_items()["href"] == (
        "http://test/stac{}/collections/naip/items".format(app.state.router_prefix)
    )


async def test_search_bbox_errors(app_client):
    body = {"query": {"bbox": [0]}}
    resp = await app_client.post("/search", json=body)
    assert resp.status_code == 400

    body = {"query": {"bbox": [100.0, 0.0, 0.0, 105.0, 1.0, 1.0]}}
    resp = await app_client.post("/search", json=body)
    assert resp.status_code == 400

    params = {"bbox": "100.0,0.0,0.0,105.0"}
    resp = await app_client.get("/search", params=params)
    assert resp.status_code == 400


async def test_preserves_extra_link(
    app_client: AsyncClient, load_test_data, load_test_collection
):
    coll = load_test_collection
    test_item = load_test_data("test_item.json")
    expected_href = urljoin(str(app_client.base_url), "preview.html")

    resp = await app_client.post(f"/collections/{coll['id']}/items", json=test_item)
    assert resp.status_code == 201

    response_item = await app_client.get(
        f"/collections/{coll['id']}/items/{test_item['id']}",
        params={"limit": 1},
    )
    assert response_item.status_code == 200
    item = response_item.json()
    extra_link = [link for link in item["links"] if link["rel"] == "preview"]
    assert extra_link
    assert extra_link[0]["href"] == expected_href


async def test_item_search_post_filter_extension_cql2_2(
    app_client, load_test_data, load_test_collection
):
    """Test POST search with JSONB query (cql json filter extension)"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    # EPSG is a JSONB key
    params = {
        "filter-lang": "cql2-json",
        "filter": {
            "op": "and",
            "args": [
                {
                    "op": "eq",
                    "args": [
                        {"property": "proj:epsg"},
                        test_item["properties"]["proj:epsg"] + 1,
                    ],
                },
                {
                    "op": "in",
                    "args": [
                        {"property": "collection"},
                        [test_item["collection"]],
                    ],
                },
            ],
        },
    }
    resp = await app_client.post("/search", json=params)
    resp_json = resp.json()

    assert resp.status_code == 200
    assert len(resp_json.get("features")) == 0

    params = {
        "filter-lang": "cql2-json",
        "filter": {
            "op": "and",
            "args": [
                {
                    "op": "eq",
                    "args": [
                        {"property": "proj:epsg"},
                        test_item["properties"]["proj:epsg"],
                    ],
                },
                {
                    "op": "in",
                    "args": [
                        {"property": "collection"},
                        [test_item["collection"]],
                    ],
                },
            ],
        },
    }
    resp = await app_client.post("/search", json=params)
    resp_json = resp.json()
    assert len(resp.json()["features"]) == 1
    assert (
        resp_json["features"][0]["properties"]["proj:epsg"]
        == test_item["properties"]["proj:epsg"]
    )


async def test_search_datetime_validation_errors(app_client):
    bad_datetimes = [
        "37-01-01T12:00:27.87Z",
        "1985-13-12T23:20:50.52Z",
        "1985-12-32T23:20:50.52Z",
        "1985-12-01T25:20:50.52Z",
        "1985-12-01T00:60:50.52Z",
        "1985-12-01T00:06:61.52Z",
        "1990-12-31T23:59:61Z",
        "1986-04-12T23:20:50.52Z/1985-04-12T23:20:50.52Z",
    ]
    for dt in bad_datetimes:
        body = {"query": {"datetime": dt}}
        resp = await app_client.post("/search", json=body)
        assert resp.status_code == 400

        resp = await app_client.get("/search?datetime={}".format(dt))
        assert resp.status_code == 400


async def test_get_filter_cql2text(app_client, load_test_data, load_test_collection):
    """Test GET search with cql2-text"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    epsg = test_item["properties"]["proj:epsg"]
    collection = test_item["collection"]

    filter = f"proj:epsg={epsg} AND collection = '{collection}'"
    params = {"filter": filter, "filter-lang": "cql2-text"}
    resp = await app_client.get("/search", params=params)
    resp_json = resp.json()
    assert len(resp.json()["features"]) == 1
    assert (
        resp_json["features"][0]["properties"]["proj:epsg"]
        == test_item["properties"]["proj:epsg"]
    )

    filter = f"proj:epsg={epsg + 1} AND collection = '{collection}'"
    params = {"filter": filter, "filter-lang": "cql2-text"}
    resp = await app_client.get("/search", params=params)
    resp_json = resp.json()
    assert len(resp.json()["features"]) == 0

    filter = f"proj:epsg={epsg}"
    params = {"filter": filter, "filter-lang": "cql2-text"}
    resp = await app_client.get(
        f"/collections/{test_item['collection']}/items", params=params
    )
    resp_json = resp.json()
    assert len(resp.json()["features"]) == 1


async def test_item_merge_raster_bands(
    app_client, load_test2_item, load_test2_collection
):
    resp = await app_client.get("/collections/test2-collection/items/test2-item")
    resp_json = resp.json()
    red_bands = resp_json["assets"]["red"]["raster:bands"]

    # The merged item should have merged the band dicts from base and item
    # into a single dict
    assert len(red_bands) == 1
    # The merged item should have the full 6 bands
    assert len(red_bands[0].keys()) == 6
    # The merged item should have kept the item value rather than the base value
    assert red_bands[0]["offset"] == 2.03976


@pytest.mark.asyncio
async def test_get_collection_items_forwarded_header(
    app_client, load_test_collection, load_test_item
):
    coll = load_test_collection
    resp = await app_client.get(
        f"/collections/{coll['id']}/items",
        headers={"Forwarded": "proto=https;host=test:1234"},
    )
    for link in resp.json()["features"][0]["links"]:
        assert link["href"].startswith("https://test:1234/")


@pytest.mark.asyncio
async def test_get_collection_items_x_forwarded_headers(
    app_client, load_test_collection, load_test_item
):
    coll = load_test_collection
    resp = await app_client.get(
        f"/collections/{coll['id']}/items",
        headers={
            "X-Forwarded-Port": "1234",
            "X-Forwarded-Proto": "https",
        },
    )
    for link in resp.json()["features"][0]["links"]:
        assert link["href"].startswith("https://test:1234/")


@pytest.mark.asyncio
async def test_get_collection_items_duplicate_forwarded_headers(
    app_client, load_test_collection, load_test_item
):
    coll = load_test_collection
    resp = await app_client.get(
        f"/collections/{coll['id']}/items",
        headers={
            "Forwarded": "proto=https;host=test:1234",
            "X-Forwarded-Port": "4321",
            "X-Forwarded-Proto": "http",
        },
    )
    for link in resp.json()["features"][0]["links"]:
        assert link["href"].startswith("https://test:1234/")


async def test_get_filter_extension(app_client, load_test_data, load_test_collection):
    """Test GET with Filter extension"""
    test_item = load_test_data("test_item.json")
    collection_id = test_item["collection"]
    ids = []

    # Ingest 5 items
    for _ in range(5):
        uid = str(uuid.uuid4())
        test_item["id"] = uid
        resp = await app_client.post(
            f"/collections/{collection_id}/items", json=test_item
        )
        assert resp.status_code == 201
        ids.append(uid)

    search_id = ids[2]

    # SEARCH
    # CQL2-JSON
    resp = await app_client.get(
        "/search",
        params={
            "filter-lang": "cql2-json",
            "filter": json.dumps({"op": "in", "args": [{"property": "id"}, [search_id]]}),
        },
    )
    assert resp.status_code == 200
    fc = resp.json()
    assert len(fc["features"]) == 1
    assert fc["features"][0]["id"] == search_id

    # CQL2-TEXT
    resp = await app_client.get(
        "/search",
        params={
            "filter-lang": "cql2-text",
            "filter": f"id='{search_id}'",
        },
    )
    assert resp.status_code == 200
    fc = resp.json()
    assert len(fc["features"]) == 1
    assert fc["features"][0]["id"] == search_id

    # ITEM COLLECTION
    # CQL2-JSON
    resp = await app_client.get(
        f"/collections/{collection_id}/items",
        params={
            "filter-lang": "cql2-json",
            "filter": json.dumps({"op": "in", "args": [{"property": "id"}, [search_id]]}),
        },
    )
    assert resp.status_code == 200
    fc = resp.json()
    assert len(fc["features"]) == 1
    assert fc["features"][0]["id"] == search_id

    # CQL2-TEXT
    resp = await app_client.get(
        f"/collections/{collection_id}/items",
        params={
            "filter-lang": "cql2-text",
            "filter": f"id='{search_id}'",
        },
    )
    assert resp.status_code == 200
    fc = resp.json()
    assert len(fc["features"]) == 1
    assert fc["features"][0]["id"] == search_id


async def test_get_search_link_media(app_client):
    """Test Search request returned links"""
    # GET
    resp = await app_client.get("/search")
    assert resp.status_code == 200
    links = resp.json()["links"]
    assert len(links) == 2
    get_self_link = next((link for link in links if link["rel"] == "self"), None)
    assert get_self_link["type"] == "application/geo+json"

    # POST
    resp = await app_client.post("/search", json={})
    assert resp.status_code == 200
    links = resp.json()["links"]
    assert len(links) == 2
    get_self_link = next((link for link in links if link["rel"] == "self"), None)
    assert get_self_link["type"] == "application/geo+json"


@pytest.mark.asyncio
async def test_item_search_freetext(app_client, load_test_data, load_test_collection):
    res = await app_client.get("/_mgmt/health")
    pgstac_version = res.json()["pgstac"]["pgstac_version"]
    if tuple(map(int, pgstac_version.split("."))) < (0, 9, 2):
        pytest.skip("Need PgSTAC > 0.9.2")

    test_item = load_test_data("test_item.json")
    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 201

    # free-text
    resp = await app_client.get(
        "/search",
        params={"q": "orthorectified"},
    )
    assert resp.json()["numberReturned"] == 1
    assert resp.json()["features"][0]["id"] == "test-item"

    resp = await app_client.get(
        "/search",
        params={"q": "orthorectified,yo"},
    )
    assert resp.json()["numberReturned"] == 1
    assert resp.json()["features"][0]["id"] == "test-item"

    resp = await app_client.get(
        "/search",
        params={"q": "yo"},
    )
    assert resp.json()["numberReturned"] == 0


@pytest.mark.asyncio
async def test_item_asset_change(app_client, load_test_data):
    """Check that changing item_assets in collection does
    not affect existing items if hydration should not occur.

    """
    # load collection
    data = load_test_data("test2_collection.json")
    collection_id = data["id"]

    resp = await app_client.post("/collections", json=data)
    assert "item_assets" in data
    assert resp.status_code == 201
    assert "item_assets" in resp.json()

    # load items
    test_item = load_test_data("test2_item.json")
    resp = await app_client.post(f"/collections/{collection_id}/items", json=test_item)
    assert resp.status_code == 201

    # check list of items
    resp = await app_client.get(
        f"/collections/{collection_id}/items", params={"limit": 1}
    )
    assert len(resp.json()["features"]) == 1
    assert resp.status_code == 200

    # NOTE: API or PgSTAC Hydration we should get the same values as original Item
    assert (
        test_item["assets"]["red"]["raster:bands"]
        == resp.json()["features"][0]["assets"]["red"]["raster:bands"]
    )

    # NOTE: `description` is not in the item body but in the collection's item-assets
    # because it's not in the original item it won't be hydrated
    assert not resp.json()["features"][0]["assets"]["qa_pixel"].get("description")

    ###########################################################################
    # Remove item_assets in collection
    operations = [{"op": "remove", "path": "/item_assets"}]
    resp = await app_client.patch(f"/collections/{collection_id}", json=operations)
    assert resp.status_code == 200

    # Make sure item_assets is not in collection response
    resp = await app_client.get(f"/collections/{collection_id}")
    assert resp.status_code == 200
    assert "item_assets" not in resp.json()
    ###########################################################################

    resp = await app_client.get(
        f"/collections/{collection_id}/items", params={"limit": 1}
    )
    assert len(resp.json()["features"]) == 1
    assert resp.status_code == 200

    # NOTE: here we should only get `scale`, `offset` and `spatial_resolution`
    # because the other values were stripped on ingestion (dehydration is a default in PgSTAC)
    # scale and offset are no in item-asset and spatial_resolution is different, so the value in the item body is kept
    assert ["scale", "offset", "spatial_resolution"] == list(
        resp.json()["features"][0]["assets"]["red"]["raster:bands"][0]
    )

    # Only run this test for PgSTAC hydratation because `exclude_hydrate_markers=True` by default
    if not app_client._transport.app.state.settings.use_api_hydrate:
        # NOTE: `description` is not in the original item but in the collection's item-assets
        # We get "𒍟※" because PgSTAC set it when ingesting (`description`is item-assets)
        # because we removed item-assets, pgstac cannot hydrate this field, and thus return "𒍟※"
        assert resp.json()["features"][0]["assets"]["qa_pixel"]["description"] == "𒍟※"

@pytest.mark.parametrize("usecase", ("ok1", "ok2", "ko"))
async def test_pagination_different_collections(app_client, load_test_data, usecase):
    """
    This test demonstrates an error case ("ko" use case): when the same item exists in two collections, the
    /search endpoint without 'limit' parameter returns the expected result = all items from all collections.
    But when we add a 'limit' parameter, the /search endpoint fails: it returns only items from the first collection,
    with a 'next' link that should allow to get items from the next collections, but this 'next' link doesn't work: 
    it returns nothing.

    This test also implements two nominal use cases that work as expected ("ok1" and "ok2"): when two different items
    exist in the same collection, and when two different items exist in two different collections.
    """

    col1_data = load_test_data("test_collection.json")
    col2_data = load_test_data("test2_collection.json")

    item1_data = load_test_data("test_item.json")
    item2_data = load_test_data("test_item2.json")

    # KO use case: the same item exists in two different collections
    if usecase == "ko":
        assert (await app_client.post("/collections", json=col1_data)).status_code == 201
        assert (await app_client.post("/collections", json=col2_data)).status_code == 201

        for col_data in col1_data, col2_data:
            item1_data["collection"] = col_data["id"]
            assert (await app_client.post(f"/collections/{col_data['id']}/items", json=item1_data)).status_code == 201

    # First OK use case: two different items in the same collection
    elif usecase == "ok1":
        assert (await app_client.post("/collections", json=col1_data)).status_code == 201

        for item_data in item1_data, item2_data:
            item_data["collection"] = col1_data["id"]
            assert (await app_client.post(f"/collections/{col1_data['id']}/items", json=item_data)).status_code == 201

    # Second OK use case: two different items in two different collections
    elif usecase == "ok2":
        assert (await app_client.post("/collections", json=col1_data)).status_code == 201
        assert (await app_client.post("/collections", json=col2_data)).status_code == 201
        
        item1_data["collection"] = col1_data["id"]
        assert (await app_client.post(f"/collections/{col1_data['id']}/items", json=item1_data)).status_code == 201
        
        item2_data["collection"] = col2_data["id"]
        assert (await app_client.post(f"/collections/{col2_data['id']}/items", json=item2_data)).status_code == 201

    # Call the /search endpoint without parameters. 
    # The result is always as expected in all use cases: two features are returned.
    resp = await app_client.get("/search")
    assert resp.status_code == 200
    contents = resp.json()
    assert contents["numberReturned"] == 2
    all_features = contents["features"] # save returned features

    # Call the /search endpoint again, but this time return only one feature per page
    resp = await app_client.get("/search", params={"limit": 1})
    contents = None

    # Loop on all pages/features. For each page, we will: 
    # - check the returned feature contents
    # - check the previous link contents, if any
    # - get the next feature
    for page, expected_feature in enumerate(all_features):
        page += 1
        is_first_page = (page == 1)
        is_last_page = (page == len(all_features))

        # Get returned feature contents, save old feature contents
        previous_contents = contents
        assert resp.status_code == 200
        contents = resp.json()

        # Get the previous and next links
        links = contents["links"]
        previous_urls = [link["href"] for link in links if link["rel"] == "previous"]
        next_urls = [link["href"] for link in links if link["rel"] == "next"]

        # Check that they are present, except for:
        # - the first page has no previous link
        # - the last page has no next link
        if is_first_page:
            assert len(previous_urls) == 0
            previous_url = None
        else:
            assert len(previous_urls) == 1
            # NOTE: in the "ko" use case, the "previous" url seems invalid: http://test/search?limit=1&token=prev%3A%3A
            # It should be like: http://test/search?limit=1&token=prev%3Atest-collection%3Atest-item
            previous_url = previous_urls[0]

        if is_last_page:
            assert len(next_urls) == 0
            next_url = None
        else:
            assert len(next_urls) == 1
            next_url = next_urls[0]

        print(f"""
page: {page}
previous url: {previous_url}
next url: {next_url}
numberReturned: {contents['numberReturned']}""")

        # A single feature should always be returned. Check its contents.
        assert contents["numberReturned"] == 1
        feature = contents["features"][0]
        print(f"returned feature: {feature['collection']}:{feature['id']}")
        assert expected_feature == feature

        # Check the previous link contents
        if not is_first_page:
            assert previous_contents == (await app_client.get(previous_url)).json()

        # Get the next feature
        if not is_last_page:
            resp = await app_client.get(next_url)