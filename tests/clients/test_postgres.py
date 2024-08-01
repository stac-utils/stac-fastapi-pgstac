import logging
import uuid
from contextlib import asynccontextmanager
from copy import deepcopy
from typing import Callable, Literal

import pytest
from fastapi import Request
from stac_pydantic import Collection, Item

from stac_fastapi.pgstac.db import close_db_connection, connect_to_db, get_connection

# from tests.conftest import MockStarletteRequest
logger = logging.getLogger(__name__)


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


async def test_update_collection(app_client, load_test_collection, load_test_data):
    in_coll = load_test_collection
    in_coll["keywords"].append("newkeyword")

    resp = await app_client.put(f"/collections/{in_coll['id']}", json=in_coll)
    assert resp.status_code == 200

    resp = await app_client.get(f"/collections/{in_coll['id']}")
    assert resp.status_code == 200

    get_coll = Collection.model_validate(resp.json())
    in_coll = Collection(**in_coll)
    assert in_coll.model_dump(exclude={"links"}) == get_coll.model_dump(exclude={"links"})
    assert "newkeyword" in get_coll.keywords


async def test_delete_collection(app_client, load_test_collection):
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


async def test_create_item_no_collection_id(
    app_client, load_test_data: Callable, load_test_collection
):
    """Items with no collection id should be set with the collection id from the path"""
    coll = load_test_collection

    item = load_test_data("test_item.json")
    item["collection"] = None

    resp = await app_client.post(
        f"/collections/{coll['id']}/items",
        json=item,
    )

    assert resp.status_code == 201

    resp = await app_client.get(f"/collections/{coll['id']}/items/{item['id']}")

    assert resp.status_code == 200

    get_item = Item.model_validate(resp.json())
    assert get_item.collection == coll["id"]


async def test_create_item_invalid_ids(
    app_client, load_test_data: Callable, load_test_collection
):
    """Items with invalid ids should return an error"""
    coll = load_test_collection

    item = load_test_data("test_item.json")
    item["id"] = "invalid/id"
    resp = await app_client.post(
        f"/collections/{coll['id']}/items",
        json=item,
    )
    assert resp.status_code == 400


async def test_create_item_invalid_collection_id(
    app_client, load_test_data: Callable, load_test_collection
):
    """Items with invalid collection ids should return an error"""
    coll = load_test_collection

    item = load_test_data("test_item.json")
    item["collection"] = "wrong-collection-id"
    resp = await app_client.post(
        f"/collections/{coll['id']}/items",
        json=item,
    )
    assert resp.status_code == 400


async def test_create_item_bad_body(
    app_client, load_test_data: Callable, load_test_collection
):
    """Items with invalid type should return an error"""
    coll = load_test_collection

    item = load_test_data("test_item.json")
    item["type"] = "not-a-type"
    resp = await app_client.post(
        f"/collections/{coll['id']}/items",
        json=item,
    )
    assert resp.status_code == 400


async def test_update_item(app_client, load_test_collection, load_test_item):
    coll = load_test_collection
    item = load_test_item

    item["properties"]["description"] = "Update Test"

    resp = await app_client.put(
        f"/collections/{coll['id']}/items/{item['id']}", json=item
    )
    assert resp.status_code == 200

    resp = await app_client.get(f"/collections/{coll['id']}/items/{item['id']}")
    assert resp.status_code == 200
    get_item = Item.model_validate(resp.json())

    item = Item(**item)
    assert item.model_dump(exclude={"links"}) == get_item.model_dump(exclude={"links"})
    assert get_item.properties.description == "Update Test"


async def test_delete_item(app_client, load_test_collection, load_test_item):
    coll = load_test_collection
    item = load_test_item

    resp = await app_client.delete(f"/collections/{coll['id']}/items/{item['id']}")
    assert resp.status_code == 200

    resp = await app_client.get(f"/collections/{coll['id']}/items/{item['id']}")
    assert resp.status_code == 404


async def test_get_collection_items(app_client, load_test_collection, load_test_item):
    coll = load_test_collection
    item = load_test_item

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


async def test_create_item_collection(
    app_client, load_test_data: Callable, load_test_collection
):
    """POSTing a FeatureCollection to the items endpoint should create the items"""
    coll = load_test_collection
    base_item = load_test_data("test_item.json")

    items = []
    for _ in range(5):
        item = deepcopy(base_item)
        item["id"] = str(uuid.uuid4())
        items.append(item)

    item_collection = {"type": "FeatureCollection", "features": items, "links": []}

    resp = await app_client.post(
        f"/collections/{coll['id']}/items",
        json=item_collection,
    )

    assert resp.status_code == 201

    resp = await app_client.get(
        f"/collections/{coll['id']}/items",
    )
    for item in items:
        resp = await app_client.get(f"/collections/{coll['id']}/items/{item['id']}")
        assert resp.status_code == 200


async def test_create_item_collection_no_collection_ids(
    app_client, load_test_data: Callable, load_test_collection
):
    """Items in ItemCollection with no collection ids should be set with the collection id from the path"""
    coll = load_test_collection
    base_item = load_test_data("test_item.json")

    items = []
    for _ in range(5):
        item = deepcopy(base_item)
        item["id"] = str(uuid.uuid4())
        item["collection"] = None
        items.append(item)

    item_collection = {"type": "FeatureCollection", "features": items, "links": []}

    resp = await app_client.post(
        f"/collections/{coll['id']}/items",
        json=item_collection,
    )

    assert resp.status_code == 201

    resp = await app_client.get(
        f"/collections/{coll['id']}/items",
    )
    for item in items:
        resp = await app_client.get(f"/collections/{coll['id']}/items/{item['id']}")
        assert resp.status_code == 200
        assert resp.json()["collection"] == coll["id"]


async def test_create_item_collection_invalid_collection_ids(
    app_client, load_test_data: Callable, load_test_collection
):
    """Feature collection containing items with invalid collection ids should return an error"""
    coll = load_test_collection
    base_item = load_test_data("test_item.json")

    items = []
    for _ in range(5):
        item = deepcopy(base_item)
        item["id"] = str(uuid.uuid4())
        item["collection"] = "wrong-collection-id"
        items.append(item)

    item_collection = {"type": "FeatureCollection", "features": items, "links": []}

    resp = await app_client.post(
        f"/collections/{coll['id']}/items",
        json=item_collection,
    )

    assert resp.status_code == 400


async def test_create_item_collection_invalid_item_ids(
    app_client, load_test_data: Callable, load_test_collection
):
    """Feature collection containing items with invalid ids should return an error"""
    coll = load_test_collection
    base_item = load_test_data("test_item.json")

    items = []
    for _ in range(5):
        item = deepcopy(base_item)
        item["id"] = str(uuid.uuid4()) + "/bad/id"
        items.append(item)

    item_collection = {"type": "FeatureCollection", "features": items, "links": []}

    resp = await app_client.post(
        f"/collections/{coll['id']}/items",
        json=item_collection,
    )

    assert resp.status_code == 400


async def test_create_bulk_items(
    app_client, load_test_data: Callable, load_test_collection
):
    coll = load_test_collection
    item = load_test_data("test_item.json")

    items = {}
    for _ in range(2):
        _item = deepcopy(item)
        _item["id"] = str(uuid.uuid4())
        items[_item["id"]] = _item

    payload = {"items": items}

    resp = await app_client.post(
        f"/collections/{coll['id']}/bulk_items",
        json=payload,
    )
    assert resp.status_code == 200
    assert resp.text == '"Successfully added 2 items."'

    for item_id in items.keys():
        resp = await app_client.get(f"/collections/{coll['id']}/items/{item_id}")
        assert resp.status_code == 200


async def test_create_bulk_items_already_exist_insert(
    app_client, load_test_data: Callable, load_test_collection
):
    coll = load_test_collection
    item = load_test_data("test_item.json")

    items = {}
    for _ in range(2):
        _item = deepcopy(item)
        _item["id"] = str(uuid.uuid4())
        items[_item["id"]] = _item

    payload = {"items": items, "method": "insert"}

    resp = await app_client.post(
        f"/collections/{coll['id']}/bulk_items",
        json=payload,
    )
    assert resp.status_code == 200
    assert resp.text == '"Successfully added 2 items."'

    for item_id in items.keys():
        resp = await app_client.get(f"/collections/{coll['id']}/items/{item_id}")
        assert resp.status_code == 200

    # Try creating the same items again.
    # This should fail with the default insert behavior.
    resp = await app_client.post(
        f"/collections/{coll['id']}/bulk_items",
        json=payload,
    )
    assert resp.status_code == 409


async def test_create_bulk_items_already_exist_upsert(
    app_client, load_test_data: Callable, load_test_collection
):
    coll = load_test_collection
    item = load_test_data("test_item.json")

    items = {}
    for _ in range(2):
        _item = deepcopy(item)
        _item["id"] = str(uuid.uuid4())
        items[_item["id"]] = _item

    payload = {"items": items, "method": "insert"}

    resp = await app_client.post(
        f"/collections/{coll['id']}/bulk_items",
        json=payload,
    )
    assert resp.status_code == 200
    assert resp.text == '"Successfully added 2 items."'

    for item_id in items.keys():
        resp = await app_client.get(f"/collections/{coll['id']}/items/{item_id}")
        assert resp.status_code == 200

    # Try creating the same items again, but using upsert.
    # This should succeed.
    payload["method"] = "upsert"
    resp = await app_client.post(
        f"/collections/{coll['id']}/bulk_items",
        json=payload,
    )
    assert resp.status_code == 200
    assert resp.text == '"Successfully upserted 2 items."'


async def test_create_bulk_items_omit_collection(
    app_client, load_test_data: Callable, load_test_collection
):
    coll = load_test_collection
    item = load_test_data("test_item.json")

    items = {}
    for _ in range(2):
        _item = deepcopy(item)
        _item["id"] = str(uuid.uuid4())
        # remove collection ID here
        del _item["collection"]
        items[_item["id"]] = _item

    payload = {"items": items, "method": "insert"}

    resp = await app_client.post(
        f"/collections/{coll['id']}/bulk_items",
        json=payload,
    )
    assert resp.status_code == 200
    assert resp.text == '"Successfully added 2 items."'

    for item_id in items.keys():
        resp = await app_client.get(f"/collections/{coll['id']}/items/{item_id}")
        assert resp.status_code == 200

    # Try creating the same items again, but using upsert.
    # This should succeed.
    payload["method"] = "upsert"
    resp = await app_client.post(
        f"/collections/{coll['id']}/bulk_items",
        json=payload,
    )
    assert resp.status_code == 200
    assert resp.text == '"Successfully upserted 2 items."'


async def test_create_bulk_items_collection_mismatch(
    app_client, load_test_data: Callable, load_test_collection
):
    coll = load_test_collection
    item = load_test_data("test_item.json")

    items = {}
    for _ in range(2):
        _item = deepcopy(item)
        _item["id"] = str(uuid.uuid4())
        _item["collection"] = "wrong-collection"
        items[_item["id"]] = _item

    payload = {"items": items, "method": "insert"}

    resp = await app_client.post(
        f"/collections/{coll['id']}/bulk_items",
        json=payload,
    )
    assert resp.status_code == 400
    assert (
        resp.json()["detail"]
        == "Collection ID from path parameter (test-collection) does not match Collection ID from Item (wrong-collection)"
    )


async def test_create_bulk_items_id_mismatch(
    app_client, load_test_data: Callable, load_test_collection
):
    coll = load_test_collection
    item = load_test_data("test_item.json")

    items = {}
    for _ in range(2):
        _item = deepcopy(item)
        _item["id"] = str(uuid.uuid4())
        _item["collection"] = "wrong-collection"
        items[_item["id"] + "wrong"] = _item

    payload = {"items": items, "method": "insert"}

    resp = await app_client.post(
        f"/collections/{coll['id']}/bulk_items",
        json=payload,
    )
    assert resp.status_code == 400
    assert (
        resp.json()["detail"]
        == "Collection ID from path parameter (test-collection) does not match Collection ID from Item (wrong-collection)"
    )


# TODO since right now puts implement upsert
# test_create_collection_already_exists
# test create_item_already_exists


# def test_get_collection_items(
#     postgres_core: CoreCrudClient,
#     postgres_transactions: TransactionsClient,
#     load_test_data: Callable,
# ):
#     coll = Collection.model_validate(load_test_data("test_collection.json"))
#     postgres_transactions.create_collection(coll, request=MockStarletteRequest)

#     item = Item.model_validate(load_test_data("test_item.json"))

#     for _ in range(5):
#         item.id = str(uuid.uuid4())
#         postgres_transactions.create_item(item, request=MockStarletteRequest)

#     fc = postgres_core.item_collection(coll.id, request=MockStarletteRequest)
#     assert len(fc.features) == 5

#     for item in fc.features:
#         assert item.collection == coll.id


@asynccontextmanager
async def custom_get_connection(
    request: Request,
    readwrite: Literal["r", "w"],
):
    """An example of customizing the connection getter"""
    async with get_connection(request, readwrite) as conn:
        await conn.execute("SELECT set_config('api.test', 'added-config', false)")
        yield conn


class TestDbConnect:
    @pytest.fixture
    async def app(self, api_client):
        """
        app fixture override to setup app with a customized db connection getter
        """
        logger.debug("Customizing app setup")
        await connect_to_db(api_client.app, custom_get_connection)
        yield api_client.app
        await close_db_connection(api_client.app)

    async def test_db_setup(self, api_client, app_client):
        @api_client.app.get(f"{api_client.router.prefix}/db-test")
        async def example_view(request: Request):
            async with request.app.state.get_connection(request, "r") as conn:
                return await conn.fetchval("SELECT current_setting('api.test', true)")

        response = await app_client.get("/db-test")
        assert response.status_code == 200
        assert response.json() == "added-config"
