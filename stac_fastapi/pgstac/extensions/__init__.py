"""pgstac extension customisations."""

from typing import TYPE_CHECKING

from .filter import FiltersClient
from .free_text import FreeTextExtension
from .query import QueryExtension

if TYPE_CHECKING:
    from .catalogs.catalogs_client import CatalogsClient as CatalogsClient
    from .catalogs.catalogs_database_logic import (
        CatalogsDatabaseLogic as CatalogsDatabaseLogic,
    )
else:
    try:
        from .catalogs.catalogs_client import CatalogsClient as CatalogsClient
        from .catalogs.catalogs_database_logic import (
            CatalogsDatabaseLogic as CatalogsDatabaseLogic,
        )
    except ImportError:
        CatalogsClient = None  # type: ignore [assignment]
        CatalogsDatabaseLogic = None  # type: ignore [assignment]

__all__ = [
    "QueryExtension",
    "FiltersClient",
    "FreeTextExtension",
    "CatalogsClient",
    "CatalogsDatabaseLogic",
]
