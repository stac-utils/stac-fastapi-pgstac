"""FastAPI application using PGStac.

Enables the extensions specified as a comma-delimited list in
the ENABLED_EXTENSIONS environment variable (e.g. `transactions,sort,query`).
If the variable is not set, enables all extensions.
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import cast

from brotli_asgi import BrotliMiddleware
from fastapi import APIRouter, FastAPI
from stac_fastapi.api.app import StacApi
from stac_fastapi.api.middleware import ProxyHeaderMiddleware
from stac_fastapi.api.models import (
    EmptyRequest,
    ItemCollectionUri,
    JSONResponse,
    create_get_request_model,
    create_post_request_model,
    create_request_model,
)
from stac_fastapi.extensions.core import (
    CollectionSearchExtension,
    CollectionSearchFilterExtension,
    FieldsExtension,
    ItemCollectionFilterExtension,
    OffsetPaginationExtension,
    SearchFilterExtension,
    SortExtension,
    TokenPaginationExtension,
    TransactionExtension,
)
from stac_fastapi.extensions.core.fields import FieldsConformanceClasses
from stac_fastapi.extensions.core.free_text import FreeTextConformanceClasses
from stac_fastapi.extensions.core.query import QueryConformanceClasses
from stac_fastapi.extensions.core.sort import SortConformanceClasses
from stac_fastapi.extensions.third_party import BulkTransactionExtension
from stac_fastapi.types.extension import ApiExtension
from stac_fastapi.types.search import APIRequest
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

from stac_fastapi.pgstac.config import Settings
from stac_fastapi.pgstac.core import CoreCrudClient, health_check
from stac_fastapi.pgstac.db import close_db_connection, connect_to_db
from stac_fastapi.pgstac.extensions import (
    CatalogsDatabaseLogic,
    FreeTextExtension,
    QueryExtension,
)
from stac_fastapi.pgstac.extensions.catalogs.catalogs_client import CatalogsClient
from stac_fastapi.pgstac.extensions.filter import FiltersClient
from stac_fastapi.pgstac.transactions import BulkTransactionsClient, TransactionsClient
from stac_fastapi.pgstac.types.search import PgstacSearch

logger = logging.getLogger(__name__)

# Optional catalogs extension (optional dependency)
try:
    from stac_fastapi_catalogs_extension import CatalogsExtension
except ImportError:
    CatalogsExtension = None

settings = Settings()


def _is_env_flag_enabled(name: str) -> bool:
    """Return True if the given env var is enabled.

    Accepts common truthy values ("yes", "true", "1") case-insensitively.
    """
    return os.environ.get(name, "").lower() in ("yes", "true", "1")


# search extensions
search_extensions_map: dict[str, ApiExtension] = {
    "query": QueryExtension(),
    "sort": SortExtension(),
    "fields": FieldsExtension(),
    "filter": SearchFilterExtension(client=FiltersClient()),
    "pagination": TokenPaginationExtension(),
}

# collection_search extensions
cs_extensions_map: dict[str, ApiExtension] = {
    "query": QueryExtension(conformance_classes=[QueryConformanceClasses.COLLECTIONS]),
    "sort": SortExtension(conformance_classes=[SortConformanceClasses.COLLECTIONS]),
    "fields": FieldsExtension(conformance_classes=[FieldsConformanceClasses.COLLECTIONS]),
    "filter": CollectionSearchFilterExtension(client=FiltersClient()),
    "free_text": FreeTextExtension(
        conformance_classes=[FreeTextConformanceClasses.COLLECTIONS],
    ),
    "pagination": OffsetPaginationExtension(),
}

# item_collection extensions
itm_col_extensions_map: dict[str, ApiExtension] = {
    "query": QueryExtension(
        conformance_classes=[QueryConformanceClasses.ITEMS],
    ),
    "sort": SortExtension(
        conformance_classes=[SortConformanceClasses.ITEMS],
    ),
    "fields": FieldsExtension(conformance_classes=[FieldsConformanceClasses.ITEMS]),
    "filter": ItemCollectionFilterExtension(client=FiltersClient()),
    "pagination": TokenPaginationExtension(),
}

enabled_extensions: set[str] = {
    *search_extensions_map.keys(),
    *cs_extensions_map.keys(),
    *itm_col_extensions_map.keys(),
    "collection_search",
}

if ext := settings.enabled_extensions:
    enabled_extensions = set(ext.split(","))

application_extensions: list[ApiExtension] = []

with_transactions = _is_env_flag_enabled("ENABLE_TRANSACTIONS_EXTENSIONS")
if with_transactions:
    application_extensions.append(
        TransactionExtension(
            client=TransactionsClient(),
            settings=settings,
            response_class=JSONResponse,
        ),
    )

    application_extensions.append(
        BulkTransactionExtension(client=BulkTransactionsClient()),
    )

# /search models
search_extensions = [
    extension
    for key, extension in search_extensions_map.items()
    if key in enabled_extensions
]
post_request_model = create_post_request_model(search_extensions, base_model=PgstacSearch)
get_request_model = create_get_request_model(search_extensions)
application_extensions.extend(search_extensions)

# /collections/{collectionId}/items model
items_get_request_model: type[APIRequest] = ItemCollectionUri
itm_col_extensions = [
    extension
    for key, extension in itm_col_extensions_map.items()
    if key in enabled_extensions
]
if itm_col_extensions:
    items_get_request_model = cast(
        type[APIRequest],
        create_request_model(
            model_name="ItemCollectionUri",
            base_model=ItemCollectionUri,
            extensions=itm_col_extensions,
            request_type="GET",
        ),
    )

    application_extensions.extend(itm_col_extensions)

# /collections model
collections_get_request_model: type[APIRequest] = EmptyRequest
if "collection_search" in enabled_extensions:
    cs_extensions = [
        extension
        for key, extension in cs_extensions_map.items()
        if key in enabled_extensions
    ]
    collection_search_extension = CollectionSearchExtension.from_extensions(cs_extensions)
    collections_get_request_model = collection_search_extension.GET
    application_extensions.append(collection_search_extension)

# Optional catalogs route
ENABLE_CATALOGS_ROUTE = _is_env_flag_enabled("ENABLE_CATALOGS_ROUTE")
logger.info("ENABLE_CATALOGS_ROUTE is set to %s", ENABLE_CATALOGS_ROUTE)

if ENABLE_CATALOGS_ROUTE:
    if CatalogsExtension is None:
        logger.warning(
            "ENABLE_CATALOGS_ROUTE is set to true, but the catalogs extension is not installed. "
            "Please install it with: pip install stac-fastapi-core[catalogs].",
        )
    else:
        try:
            catalogs_extension = CatalogsExtension(
                client=CatalogsClient(database=CatalogsDatabaseLogic()),
                enable_transactions=with_transactions,
            )
            application_extensions.append(catalogs_extension)
            logger.info("CatalogsExtension enabled successfully.")
        except Exception as e:  # pragma: no cover - defensive
            logger.warning("Failed to initialize CatalogsExtension: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI Lifespan."""
    await connect_to_db(app, add_write_connection_pool=with_transactions)
    yield
    await close_db_connection(app)


api = StacApi(
    app=FastAPI(
        openapi_url=settings.openapi_url,
        docs_url=settings.docs_url,
        redoc_url=None,
        root_path=settings.root_path,
        title=settings.stac_fastapi_title,
        version=settings.stac_fastapi_version,
        description=settings.stac_fastapi_description,
        lifespan=lifespan,
    ),
    router=APIRouter(prefix=settings.prefix_path),
    settings=settings,
    extensions=application_extensions,
    client=CoreCrudClient(pgstac_search_model=post_request_model),  # type: ignore [arg-type]
    response_class=JSONResponse,
    items_get_request_model=items_get_request_model,
    search_get_request_model=get_request_model,
    search_post_request_model=post_request_model,
    collections_get_request_model=collections_get_request_model,
    middlewares=[
        Middleware(BrotliMiddleware),
        Middleware(ProxyHeaderMiddleware),
        Middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_origin_regex=settings.cors_origin_regex,
            allow_methods=settings.cors_methods,
            allow_credentials=settings.cors_credentials,
            allow_headers=settings.cors_headers,
            max_age=600,
        ),
    ],
    health_check=health_check,  # type: ignore [arg-type]
)
app = api.app


def run():
    """Run app from command line using uvicorn if available."""
    try:
        import uvicorn

        uvicorn.run(
            "stac_fastapi.pgstac.app:app",
            host=settings.app_host,
            port=settings.app_port,
            log_level="info",
            reload=settings.reload,
            root_path=settings.uvicorn_root_path,
        )
    except ImportError as e:
        raise RuntimeError("Uvicorn must be installed in order to use command") from e


if __name__ == "__main__":
    run()
