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
    FilterExtension,
    OffsetPaginationExtension,
    SortExtension,
    TokenPaginationExtension,
    TransactionExtension,
)
from stac_fastapi.extensions.core.collection_search import CollectionSearchExtension
from stac_fastapi.extensions.third_party import BulkTransactionExtension
from starlette.middleware import Middleware

from stac_fastapi.pgstac.config import Settings
from stac_fastapi.pgstac.core import CoreCrudClient
from stac_fastapi.pgstac.db import close_db_connection, connect_to_db
from stac_fastapi.pgstac.extensions import QueryExtension
from stac_fastapi.pgstac.extensions.filter import FiltersClient
from stac_fastapi.pgstac.transactions import BulkTransactionsClient, TransactionsClient
from stac_fastapi.pgstac.types.search import PgstacSearch

settings = Settings()
extensions_map = {
    "transaction": TransactionExtension(
        client=TransactionsClient(),
        settings=settings,
        response_class=ORJSONResponse,
    ),
    "query": QueryExtension(),
    "sort": SortExtension(),
    "fields": FieldsExtension(),
    "pagination": TokenPaginationExtension(),
    "filter": FilterExtension(client=FiltersClient()),
    "bulk_transactions": BulkTransactionExtension(client=BulkTransactionsClient()),
}

# some extensions are supported in combination with the collection search extension
collection_extensions_map = {
    "query": QueryExtension(),
    "sort": SortExtension(),
    "fields": FieldsExtension(),
    "filter": FilterExtension(client=FiltersClient()),
    "pagination": OffsetPaginationExtension(),
}

enabled_extensions = (
    os.environ["ENABLED_EXTENSIONS"].split(",")
    if "ENABLED_EXTENSIONS" in os.environ
    else list(extensions_map.keys()) + ["collection_search"]
)
extensions = [
    extension for key, extension in extensions_map.items() if key in enabled_extensions
]

items_get_request_model = (
    create_request_model(
        model_name="ItemCollectionUri",
        base_model=ItemCollectionUri,
        mixins=[TokenPaginationExtension().GET],
        request_type="GET",
    )
    if any(isinstance(ext, TokenPaginationExtension) for ext in extensions)
    else ItemCollectionUri
)

collection_search_extension = (
    CollectionSearchExtension.from_extensions(
        [
            extension
            for key, extension in collection_extensions_map.items()
            if key in enabled_extensions
        ]
    )
    if "collection_search" in enabled_extensions
    else None
)

collections_get_request_model = (
    collection_search_extension.GET if collection_search_extension else EmptyRequest
)

post_request_model = create_post_request_model(extensions, base_model=PgstacSearch)
get_request_model = create_get_request_model(extensions)


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
    root_path=getattr(settings, "root_path", None),
    lifespan=lifespan,
)


api = StacApi(
    app=update_openapi(fastapp),
    settings=settings,
    extensions=extensions + [collection_search_extension]
    if collection_search_extension
    else extensions,
    client=CoreCrudClient(post_request_model=post_request_model),  # type: ignore
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
