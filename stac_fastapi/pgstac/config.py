"""Postgres API configuration."""

from typing import List, Type
from urllib.parse import quote_plus as quote

from pydantic import BaseModel
from pydantic_settings import SettingsConfigDict
from stac_fastapi.types.config import ApiSettings

from stac_fastapi.pgstac.types.base_item_cache import (
    BaseItemCache,
    DefaultBaseItemCache,
)

DEFAULT_INVALID_ID_CHARS = [
    ":",
    "/",
    "?",
    "#",
    "[",
    "]",
    "@",
    "!",
    "$",
    "&",
    "'",
    "(",
    ")",
    "*",
    "+",
    ",",
    ";",
    "=",
]


class ServerSettings(BaseModel):
    """Server runtime parameters."""

    search_path: str = "pgstac,public"
    application_name: str = "pgstac"

    model_config = SettingsConfigDict(extra="allow")


class Settings(ApiSettings):
    """Postgres-specific API settings.

    Attributes:
        postgres_user: postgres username.
        postgres_pass: postgres password.
        postgres_host_reader: hostname for the reader connection.
        postgres_host_writer: hostname for the writer connection.
        postgres_port: database port.
        postgres_dbname: database name.
        use_api_hydrate: perform hydration of stac items within stac-fastapi.
        invalid_id_chars: list of characters that are not allowed in item or collection ids.
    """

    postgres_user: str
    postgres_pass: str
    postgres_host_reader: str
    postgres_host_writer: str
    postgres_port: int
    postgres_dbname: str

    db_min_conn_size: int = 10
    db_max_conn_size: int = 10
    db_max_queries: int = 50000
    db_max_inactive_conn_lifetime: float = 300

    server_settings: ServerSettings = ServerSettings()

    use_api_hydrate: bool = False
    base_item_cache: Type[BaseItemCache] = DefaultBaseItemCache
    invalid_id_chars: List[str] = DEFAULT_INVALID_ID_CHARS

    testing: bool = False

    @property
    def reader_connection_string(self):
        """Create reader psql connection string."""
        return f"postgresql://{self.postgres_user}:{quote(self.postgres_pass)}@{self.postgres_host_reader}:{self.postgres_port}/{self.postgres_dbname}"

    @property
    def writer_connection_string(self):
        """Create writer psql connection string."""
        return f"postgresql://{self.postgres_user}:{quote(self.postgres_pass)}@{self.postgres_host_writer}:{self.postgres_port}/{self.postgres_dbname}"

    @property
    def testing_connection_string(self):
        """Create testing psql connection string."""
        return f"postgresql://{self.postgres_user}:{quote(self.postgres_pass)}@{self.postgres_host_writer}:{self.postgres_port}/pgstactestdb"

    model_config = SettingsConfigDict(
        **{**ApiSettings.model_config, **{"env_nested_delimiter": "__"}}
    )
