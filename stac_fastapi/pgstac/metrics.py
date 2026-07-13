"""Prometheus metrics with low-cardinality STAC operation labels."""

from __future__ import annotations

from typing import TYPE_CHECKING

from prometheus_client import REGISTRY, Counter, Histogram
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_fastapi_instrumentator.metrics import Info

if TYPE_CHECKING:
    from fastapi import FastAPI

_INSTRUMENTED_APPS: set[int] = set()

OPERATIONS: dict[tuple[str, str], str] = {
    ("GET", "/"): "landing",
    ("GET", "/conformance"): "conformance",
    ("GET", "/collections"): "list_collections",
    ("GET", "/collections/{collection_id}"): "get_collection",
    ("GET", "/collections/{collection_id}/items"): "list_items",
    ("GET", "/collections/{collection_id}/items/{item_id}"): "get_item",
    ("GET", "/search"): "search",
    ("POST", "/search"): "search",
    ("POST", "/collections"): "create_collection",
    ("PUT", "/collections/{collection_id}"): "edit_collection",
    ("PATCH", "/collections/{collection_id}"): "edit_collection",
    ("DELETE", "/collections/{collection_id}"): "delete_collection",
    ("POST", "/collections/{collection_id}/items"): "create_item",
    ("PUT", "/collections/{collection_id}/items/{item_id}"): "edit_item",
    ("PATCH", "/collections/{collection_id}/items/{item_id}"): "edit_item",
    ("DELETE", "/collections/{collection_id}/items/{item_id}"): "delete_item",
    ("POST", "/collections/{collection_id}/bulk_items"): "bulk",
    ("GET", "/queryables"): "queryables",
    ("GET", "/collections/{collection_id}/queryables"): "queryables",
}


def resolve_operation(method: str, route: str | None) -> str:
    """Map a request method and route template to a STAC operation label."""
    if not route or route == "none":
        return "unknown"

    operation = OPERATIONS.get((method.upper(), route))
    if operation:
        return operation

    if route.startswith("/catalogs"):
        return "catalog"
    if "bulk" in route:
        return "bulk"

    return "unknown"


def _metric_or_none(factory):
    try:
        return factory()
    except ValueError as exc:
        if "Duplicated time series in CollectorRegistry" not in str(exc):
            raise
        return None


REQUESTS = _metric_or_none(
    lambda: Counter(
        "http_requests_total",
        "Total HTTP requests by STAC operation.",
        labelnames=("operation", "method", "status"),
        registry=REGISTRY,
    )
)
LATENCY = _metric_or_none(
    lambda: Histogram(
        "http_request_duration_seconds",
        "HTTP request latency by STAC operation.",
        labelnames=("operation", "method"),
        buckets=(0.1, 0.5, 1, float("inf")),
        registry=REGISTRY,
    )
)


def record_stac_metrics(info: Info) -> None:
    """Record request count and latency using STAC operation labels."""
    route = info.request.scope.get("route")
    route_path = getattr(route, "path", None)
    operation = resolve_operation(info.method, route_path)

    if REQUESTS is not None:
        REQUESTS.labels(operation, info.method, info.modified_status).inc()
    if LATENCY is not None:
        LATENCY.labels(operation, info.method).observe(info.modified_duration)


def metrics_endpoint(app: FastAPI) -> str:
    """Return the Prometheus scrape path, aligned with other mgmt routes."""
    prefix = getattr(app.state, "router_prefix", "") or ""
    return f"{prefix}/_mgmt/metrics".replace("//", "/")


def instrument_app(app: FastAPI, endpoint: str | None = None) -> None:
    """Instrument a FastAPI app and expose Prometheus metrics."""
    if id(app) in _INSTRUMENTED_APPS:
        return

    endpoint = endpoint or metrics_endpoint(app)

    (
        Instrumentator(
            should_group_status_codes=True,
            excluded_handlers=[".*/_mgmt/.*"],
        )
        .add(record_stac_metrics)
        .instrument(app)
        .expose(app, endpoint=endpoint)
    )
    _INSTRUMENTED_APPS.add(id(app))
