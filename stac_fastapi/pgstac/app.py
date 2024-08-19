"""FastAPI application using PGStac.

Enables the extensions specified as a comma-delimited list in
the ENABLED_EXTENSIONS environment variable (e.g. `transactions,sort,query`).
If the variable is not set, enables all extensions.
"""

import os

from fastapi.responses import ORJSONResponse
from stac_fastapi.api.app import StacApi
from stac_fastapi.api.models import (
    EmptyRequest,
    ItemCollectionUri,
    create_get_request_model,
    create_post_request_model,
    create_request_model,
)
from stac_fastapi.extensions.core import (
    CollectionSearchExtension,
    FieldsExtension,
    FilterExtension,
    SortExtension,
    TokenPaginationExtension,
    TransactionExtension,
)
from stac_fastapi.extensions.core.collection_search.request import (
    BaseCollectionSearchGetRequest,
)
from stac_fastapi.extensions.third_party import BulkTransactionExtension

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

collections_extensions_map = {
    "collection_search": CollectionSearchExtension(),
}

if enabled_extensions := os.getenv("ENABLED_EXTENSIONS"):
    _enabled_extensions = enabled_extensions.split(",")
    extensions = [
        extension
        for key, extension in extensions_map.items()
        if key in _enabled_extensions
    ]
    collection_extensions = [
        extension
        for key, extension in collections_extensions_map.items()
        if key in _enabled_extensions
    ]
else:
    extensions = list(extensions_map.values())
    collection_extensions = list(collections_extensions_map.values())


if any(isinstance(ext, TokenPaginationExtension) for ext in extensions):
    items_get_request_model = create_request_model(
        model_name="ItemCollectionUri",
        base_model=ItemCollectionUri,
        mixins=[TokenPaginationExtension().GET],
        request_type="GET",
    )
else:
    items_get_request_model = ItemCollectionUri

if any(isinstance(ext, CollectionSearchExtension) for ext in collection_extensions):
    collections_get_request_model = create_request_model(
        model_name="CollectionsGetRequest",
        base_model=BaseCollectionSearchGetRequest,
        extensions=extensions,
        request_type="GET",
    )
else:
    collections_get_request_model = EmptyRequest

post_request_model = create_post_request_model(
    extensions + collection_extensions, base_model=PgstacSearch
)
get_request_model = create_get_request_model(extensions + collection_extensions)

api = StacApi(
    settings=settings,
    extensions=extensions + collection_extensions,
    client=CoreCrudClient(post_request_model=post_request_model),  # type: ignore
    response_class=ORJSONResponse,
    items_get_request_model=items_get_request_model,
    search_get_request_model=get_request_model,
    search_post_request_model=post_request_model,
    collections_get_request_model=collections_get_request_model,
)
app = api.app


@app.on_event("startup")
async def startup_event():
    """Connect to database on startup."""
    await connect_to_db(app)


@app.on_event("shutdown")
async def shutdown_event():
    """Close database connection."""
    await close_db_connection(app)


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
