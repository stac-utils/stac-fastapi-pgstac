"""Postgres API configuration."""

import json
import warnings
from typing import Annotated, Any, List, Optional, Sequence, Type
from urllib.parse import quote_plus as quote

from pydantic import BaseModel, BeforeValidator, Field, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict
from stac_fastapi.types.config import ApiSettings
from typing_extensions import Self

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
    """Server runtime parameters.

    Attributes:
        search_path: Postgres search path. Defaults to "pgstac,public".
        application_name: PgSTAC Application name. Defaults to 'pgstac'.
    """

    search_path: str = "pgstac,public"
    application_name: str = "pgstac"

    model_config = SettingsConfigDict(extra="allow")


class PostgresSettings(BaseSettings):
    """Postgres-specific API settings.

    Attributes:
        pguser: postgres username.
        pgpassword: postgres password.
        pghost: hostname for the connection.
        pgport: database port.
        pgdatabase: database name.

    """

    postgres_user: Annotated[
        Optional[str],
        Field(
            deprecated="`postgres_user` is deprecated, please use `pguser`", default=None
        ),
    ]
    postgres_pass: Annotated[
        Optional[str],
        Field(
            deprecated="`postgres_pass` is deprecated, please use `pgpassword`",
            default=None,
        ),
    ]
    postgres_host_reader: Annotated[
        Optional[str],
        Field(
            deprecated="`postgres_host_reader` is deprecated, please use `pghost`",
            default=None,
        ),
    ]
    postgres_host_writer: Annotated[
        Optional[str],
        Field(
            deprecated="`postgres_host_writer` is deprecated, please use `pghost`",
            default=None,
        ),
    ]
    postgres_port: Annotated[
        Optional[int],
        Field(
            deprecated="`postgres_port` is deprecated, please use `pgport`", default=None
        ),
    ]
    postgres_dbname: Annotated[
        Optional[str],
        Field(
            deprecated="`postgres_dbname` is deprecated, please use `pgdatabase`",
            default=None,
        ),
    ]

    pguser: str
    pgpassword: str
    pghost: str
    pgport: int
    pgdatabase: str

    db_min_conn_size: int = 1
    db_max_conn_size: int = 10
    db_max_queries: int = 50000
    db_max_inactive_conn_lifetime: float = 300

    server_settings: ServerSettings = ServerSettings()

    model_config = {"env_file": ".env", "extra": "ignore"}

    @model_validator(mode="before")
    @classmethod
    def _pg_settings_compat(cls, data: Any) -> Any:
        if isinstance(data, dict):
            compat = {
                "postgres_user": "pguser",
                "postgres_pass": "pgpassword",
                "postgres_host_reader": "pghost",
                "postgres_host_writer": "pghost",
                "postgres_port": "pgport",
                "postgres_dbname": "pgdatabase",
            }
            for old_key, new_key in compat.items():
                if val := data.get(old_key, None):
                    warnings.warn(
                        f"`{old_key}` is deprecated, please use `{new_key}`",
                        DeprecationWarning,
                        stacklevel=1,
                    )
                    data[new_key] = val

            if (pgh_reader := data.get("postgres_host_reader")) and (
                pgh_writer := data.get("postgres_host_writer")
            ):
                if pgh_reader != pgh_writer:
                    raise ValueError(
                        "In order to use different host values for reading and writing "
                        "you must explicitly provide write_postgres_settings to the connect_to_db function"
                    )

        return data

    @property
    def connection_string(self):
        """Create reader psql connection string."""
        return f"postgresql://{self.pguser}:{quote(self.pgpassword)}@{self.pghost}:{self.pgport}/{self.pgdatabase}"


def str_to_list(value: Any) -> Any:
    if isinstance(value, str):
        if value.startswith("["):
            return json.loads(value)
        else:
            return [v.strip() for v in value.split(",")]
    else:
        return value


class Settings(ApiSettings):
    """API settings.

    Attributes:
        prefix_path: An optional path prefix for the underyling FastAPI router.
        use_api_hydrate: perform hydration of stac items within stac-fastapi.
        invalid_id_chars: list of characters that are not allowed in item or collection ids.

    """

    prefix_path: str = ""
    use_api_hydrate: bool = False
    """
    When USE_API_HYDRATE=TRUE, PgSTAC database will receive `NO_HYDRATE=TRUE`

    | use_api_hydrate | nohydrate | Hydration |
    |             --- |       --- |       --- |
    |           False |     False |    PgSTAC |
    |            True |      True |       API |

    ref: https://stac-utils.github.io/pgstac/pgstac/#runtime-configurations
    """
    exclude_hydrate_markers: bool = True
    """
    In some case, PgSTAC can return `DO_NOT_MERGE_MARKER` markers (`𒍟※`).
    If `EXCLUDE_HYDRATE_MARKERS=TRUE` and `USE_API_HYDRATE=TRUE`, stac-fastapi-pgstac
    will exclude those values from the responses.
    """

    invalid_id_chars: List[str] = DEFAULT_INVALID_ID_CHARS
    base_item_cache: Type[BaseItemCache] = DefaultBaseItemCache

    validate_extensions: bool = False
    """
    Validate `stac_extensions` schemas against submitted data when creating or updated STAC objects.

    Implies that the `Transactions` extension is enabled.
    """

    cors_origins: Annotated[Sequence[str], BeforeValidator(str_to_list), NoDecode] = (
        "*",
    )
    cors_origin_regex: Optional[str] = None
    cors_methods: Annotated[Sequence[str], BeforeValidator(str_to_list), NoDecode] = (
        "GET",
        "POST",
        "OPTIONS",
    )
    cors_credentials: bool = False
    cors_headers: Annotated[Sequence[str], BeforeValidator(str_to_list), NoDecode] = (
        "Content-Type",
    )

    testing: bool = False

    @model_validator(mode="after")
    def check_origins(self) -> Self:
        if self.cors_origin_regex and "*" in self.cors_origins:
            raise ValueError(
                "Conflicting options found in API settings: `cors_origin_regex` and `*` in `cors_origins`"
            )

        return self
