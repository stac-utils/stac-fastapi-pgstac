"""FastAPI application using PGStac.

Enables the extensions specified as a comma-delimited list in
the ENABLED_EXTENSIONS environment variable (e.g. `transactions,sort,query`).
If the variable is not set, enables all extensions.
"""

import logging
from contextlib import asynccontextmanager
from typing import Type, cast

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
from stac_fastapi.types.search import APIRequest
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

from stac_fastapi.pgstac.config import Settings
from stac_fastapi.pgstac.core import CoreCrudClient, health_check
from stac_fastapi.pgstac.db import close_db_connection, connect_to_db
from stac_fastapi.pgstac.models.extensions import Extensions
from stac_fastapi.pgstac.types.search import PgstacSearch


def instantiate_api(
    settings: Settings | None = None,
    client: Type[CoreCrudClient] = CoreCrudClient,
    extensions: Extensions | None = None,
) -> StacApi:
    """Instantiate the STAC API.

    Args:
        settings: The application settings, must be an instance of `Settings`.
        client: The client class to use for the API, must be a subclass of `CoreCrudClient`.
        extensions: The extensions to use for the API, must be an instance of `Extensions`.
        Provided extensions will be merged with the default extensions.
    Returns:
        An instance of the STAC API.
    """
    settings = settings or Settings()
    extensions = extensions or Extensions()

    # /search models
    post_request_model = create_post_request_model(
        extensions.search, base_model=PgstacSearch
    )
    get_request_model = create_get_request_model(extensions.search)

    # /collections/{collectionId}/items model
    items_get_request_model: type[APIRequest] = ItemCollectionUri
    if extensions.item_collection:
        items_get_request_model = cast(
            type[APIRequest],
            create_request_model(
                model_name="ItemCollectionUri",
                base_model=ItemCollectionUri,
                extensions=extensions.item_collection,
                request_type="GET",
            ),
        )

    # /collections model
    collections_get_request_model: type[APIRequest] = EmptyRequest
    collection_search_extension = extensions.collection_search
    if collection_search_extension is not None:
        collections_get_request_model = collection_search_extension.GET

    transaction_extensions = extensions.transaction

    application_extensions = [
        *extensions.search,
        *extensions.item_collection,
        *transaction_extensions,
        *extensions.catalog,
        *extensions.extra,
    ]
    if collection_search_extension is not None:
        application_extensions.append(collection_search_extension)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """FastAPI Lifespan."""
        await connect_to_db(app, add_write_connection_pool=bool(transaction_extensions))
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
        client=client(pgstac_search_model=post_request_model),  # type: ignore [arg-type]
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

    return api


api = instantiate_api()

app = api.app


def run():
    """Run app from command line using uvicorn if available."""
    try:
        import uvicorn

        from stac_fastapi.pgstac.config import Settings

        settings = Settings()

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
