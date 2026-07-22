import pytest

from stac_fastapi.pgstac.metrics import (
    metrics_endpoint,
    register_operations,
    resolve_operation,
)


class _AppState:
    def __init__(self, router_prefix: str = ""):
        self.router_prefix = router_prefix


class _App:
    def __init__(self, router_prefix: str = ""):
        self.state = _AppState(router_prefix)


@pytest.mark.parametrize(
    ("method", "route", "prefix", "expected"),
    [
        ("GET", "/search", "", "search"),
        ("POST", "/search", "", "search"),
        ("GET", "/router_prefix/search", "/router_prefix", "search"),
        ("GET", "/collections/{collection_id}/items", "", "list_items"),
        ("GET", "/collections/{collection_id}/items/{item_id}", "", "get_item"),
        ("POST", "/collections/{collection_id}/items", "", "create_item"),
        ("PUT", "/collections/{collection_id}/items/{item_id}", "", "edit_item"),
        ("PATCH", "/collections/{collection_id}/items/{item_id}", "", "edit_item"),
        ("DELETE", "/collections/{collection_id}/items/{item_id}", "", "delete_item"),
        ("POST", "/collections/{collection_id}/bulk_items", "", "bulk"),
        ("GET", "/_mgmt/health", "", "unknown"),
        ("GET", "/_mgmt/ping", "", "unknown"),
        ("GET", "/catalogs", "", "list_catalogs"),
        ("GET", "/catalogs/{catalog_id}", "", "get_catalog"),
        (
            "GET",
            "/catalogs/{catalog_id}/collections/{collection_id}/items",
            "",
            "list_items",
        ),
        ("GET", "/catalogs/root", "", "catalog"),
        ("GET", "/router_prefix/catalogs/{catalog_id}", "/router_prefix", "get_catalog"),
        (None, None, "", "unknown"),
        ("GET", "none", "", "unknown"),
    ],
)
def test_resolve_operation(method, route, prefix, expected):
    assert resolve_operation(method or "", route, prefix=prefix) == expected


def test_register_operations_extends_map():
    register_operations({("GET", "/custom-extension"): "custom_extension"})
    try:
        assert resolve_operation("GET", "/custom-extension") == "custom_extension"
    finally:
        # Keep global OPERATIONS clean for other tests.
        from stac_fastapi.pgstac.metrics import OPERATIONS

        OPERATIONS.pop(("GET", "/custom-extension"), None)


@pytest.mark.parametrize(
    ("router_prefix", "expected"),
    [
        ("", "/_mgmt/metrics"),
        ("/api", "/api/_mgmt/metrics"),
        ("/router_prefix", "/router_prefix/_mgmt/metrics"),
    ],
)
def test_metrics_endpoint(router_prefix, expected):
    assert metrics_endpoint(_App(router_prefix)) == expected
