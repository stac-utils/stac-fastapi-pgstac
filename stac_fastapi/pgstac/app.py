"""FastAPI application using PGStac.

Enables the extensions specified as a comma-delimited list in
the ENABLED_EXTENSIONS environment variable (e.g. `transactions,sort,query`).
If the variable is not set, enables all extensions.
"""

import os
from contextlib import asynccontextmanager

from brotli_asgi import BrotliMiddleware
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from stac_fastapi.api.app import StacApi
from stac_fastapi.api.middleware import CORSMiddleware, ProxyHeaderMiddleware
from stac_fastapi.api.models import (
    EmptyRequest,
    ItemCollectionUri,
    create_get_request_model,
    create_post_request_model,
    create_request_model,
)
from stac_fastapi.api.openapi import update_openapi
from stac_fastapi.extensions.core import (
    FieldsExtension,
    FreeTextExtension,
    OffsetPaginationExtension,
    SortExtension,
    TokenPaginationExtension,
    TransactionExtension,
)
from stac_fastapi.extensions.core.collection_search import CollectionSearchExtension
from stac_fastapi.extensions.core.collection_search.request import (
    BaseCollectionSearchGetRequest,
)
from stac_fastapi.extensions.third_party import BulkTransactionExtension
from starlette.middleware import Middleware

from stac_fastapi.pgstac.config import Settings
from stac_fastapi.pgstac.core import CoreCrudClient
from stac_fastapi.pgstac.db import close_db_connection, connect_to_db
from stac_fastapi.pgstac.extensions import QueryExtension
from stac_fastapi.pgstac.extensions.filter import (
    CollectionSearchFilterExtension,
    FiltersClient,
    ItemCollectionFilterExtension,
    SearchFilterExtension,
)
from stac_fastapi.pgstac.transactions import BulkTransactionsClient, TransactionsClient
from stac_fastapi.pgstac.types.search import PgstacSearch

settings = Settings()

# application extensions
application_extensions_map = {
    "transaction": TransactionExtension(
        client=TransactionsClient(),
        settings=settings,
        response_class=ORJSONResponse,
    ),
    "bulk_transactions": BulkTransactionExtension(client=BulkTransactionsClient()),
}

# search extensions
search_extensions_map = {
    "query": QueryExtension(),
    "sort": SortExtension(),
    "fields": FieldsExtension(),
    "filter": SearchFilterExtension(client=FiltersClient()),
    "pagination": TokenPaginationExtension(),
}

# collection_search extensions
cs_extensions_map = {
    "query": QueryExtension(
        conformance_classes=[
            "https://api.stacspec.org/v1.0.0-rc.1/collection-search#query"
        ]
    ),
    "sort": SortExtension(
        conformance_classes=[
            "https://api.stacspec.org/v1.0.0-rc.1/collection-search#sort"
        ]
    ),
    "fields": FieldsExtension(
        conformance_classes=[
            "https://api.stacspec.org/v1.0.0-rc.1/collection-search#fields"
        ]
    ),
    "filter": CollectionSearchFilterExtension(client=FiltersClient()),
    "free_text": FreeTextExtension(
        conformance_classes=[
            "https://api.stacspec.org/v1.0.0-rc.1/collection-search#free-text",
        ],
    ),
    "pagination": OffsetPaginationExtension(),
}

# item_collection extensions
itm_col_extensions_map = {
    "query": QueryExtension(
        conformance_classes=["https://api.stacspec.org/v1.0.0/ogcapi-features#query"],
    ),
    "sort": SortExtension(
        conformance_classes=["https://api.stacspec.org/v1.0.0/ogcapi-features#sort"],
    ),
    "fields": FieldsExtension(
        conformance_classes=["https://api.stacspec.org/v1.0.0/ogcapi-features#fields"],
    ),
    "filter": ItemCollectionFilterExtension(client=FiltersClient()),
    "pagination": TokenPaginationExtension(),
}

known_extensions = {
    *application_extensions_map.keys(),
    *search_extensions_map.keys(),
    *cs_extensions_map.keys(),
    *itm_col_extensions_map.keys(),
    "collection_search",
}

enabled_extensions = (
    os.environ["ENABLED_EXTENSIONS"].split(",")
    if "ENABLED_EXTENSIONS" in os.environ
    else known_extensions
)

application_extensions = [
    extension
    for key, extension in application_extensions_map.items()
    if key in enabled_extensions
]

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
items_get_request_model = ItemCollectionUri
itm_col_extensions = [
    extension
    for key, extension in itm_col_extensions_map.items()
    if key in enabled_extensions
]
if itm_col_extensions:
    items_get_request_model = create_request_model(
        model_name="ItemCollectionUri",
        base_model=ItemCollectionUri,
        extensions=itm_col_extensions,
        request_type="GET",
    )
    application_extensions.extend(itm_col_extensions)

# /collections model
collections_get_request_model = EmptyRequest
if "collection_search" in enabled_extensions:
    mixins = []
    mixing_conformances = [
        "https://api.stacspec.org/v1.0.0-rc.1/collection-search",
        "http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/simple-query",
    ]
    for key, extension in cs_extensions_map.items():
        if key not in enabled_extensions:
            continue
        mixins.append(extension.GET)
        mixing_conformances.extend(extension.conformance_classes)

    collections_get_request_model = create_request_model(
        model_name="CollectionsGetRequest",
        base_model=BaseCollectionSearchGetRequest,
        mixins=mixins,
        request_type="GET",
    )
    collection_search_extension = CollectionSearchExtension(
        GET=collections_get_request_model, conformance_classes=mixing_conformances
    )
    application_extensions.append(collection_search_extension)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI Lifespan."""
    await connect_to_db(app)
    yield
    await close_db_connection(app)


fastapp = FastAPI(
    openapi_url=settings.openapi_url,
    docs_url=settings.docs_url,
    redoc_url=None,
    root_path=settings.root_path,
    lifespan=lifespan,
)


api = StacApi(
    app=update_openapi(fastapp),
    settings=settings,
    extensions=application_extensions,
    client=CoreCrudClient(pgstac_search_model=post_request_model),
    response_class=ORJSONResponse,
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
            allow_methods=settings.cors_methods,
        ),
    ],
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
            root_path=os.getenv("UVICORN_ROOT_PATH", ""),
        )
    except ImportError as e:
        raise RuntimeError("Uvicorn must be installed in order to use command") from e


if __name__ == "__main__":
    run()


def create_handler(app):
    """Create a handler to use with AWS Lambda if mangum available."""
    try:
        from mangum import Mangum

        return Mangum(app)
    except ImportError:
        return None


handler = create_handler(app)
