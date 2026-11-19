"""test config."""

import warnings

import pytest
from pytest import MonkeyPatch

from stac_fastapi.pgstac.config import PostgresSettings, Settings


async def test_pg_settings_with_env(monkeypatch):
    """Test PostgresSettings with PG* environment variables"""
    monkeypatch.setenv("PGUSER", "username")
    monkeypatch.setenv("PGPASSWORD", "password")
    monkeypatch.setenv("PGHOST", "0.0.0.0")
    monkeypatch.setenv("PGPORT", "1111")
    monkeypatch.setenv("PGDATABASE", "pgstac")
    assert PostgresSettings(_env_file=None)


async def test_pg_settings_attributes(monkeypatch):
    """Test PostgresSettings with attributes"""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        settings = PostgresSettings(
            pguser="user",
            pgpassword="password",
            pghost="0.0.0.0",
            pgport=1111,
            pgdatabase="pgstac",
            _env_file=None,
        )
        assert settings.pghost == "0.0.0.0"


@pytest.mark.parametrize(
    "env_var, expected",
    [
        ("TRUE", True),
        ("YES", True),
        ("1", True),
    ],
)
def test_settings_enable_transactions_extensions(monkeypatch, env_var, expected):
    """Test that enable_transactions_extensions is properly parsed from environment variable."""
    monkeypatch.setenv("ENABLE_TRANSACTIONS_EXTENSIONS", env_var)
    settings = Settings()
    assert settings.enable_transactions_extensions == expected


@pytest.mark.parametrize(
    "cors_origins",
    [
        "http://stac-fastapi-pgstac.test,http://stac-fastapi.test",
        '["http://stac-fastapi-pgstac.test","http://stac-fastapi.test"]',
    ],
)
def test_cors_origins(monkeypatch: MonkeyPatch, cors_origins: str) -> None:
    monkeypatch.setenv(
        "CORS_ORIGINS",
        cors_origins,
    )
    settings = Settings()
    assert settings.cors_origins == [
        "http://stac-fastapi-pgstac.test",
        "http://stac-fastapi.test",
    ]


@pytest.mark.parametrize(
    "cors_methods",
    [
        "GET,POST",
        '["GET","POST"]',
    ],
)
def test_cors_methods(monkeypatch: MonkeyPatch, cors_methods: str) -> None:
    monkeypatch.setenv(
        "CORS_METHODS",
        cors_methods,
    )
    settings = Settings()
    assert settings.cors_methods == [
        "GET",
        "POST",
    ]


@pytest.mark.parametrize(
    "cors_headers",
    [
        "Content-Type,X-Foo",
        '["Content-Type","X-Foo"]',
    ],
)
def test_cors_headers(monkeypatch: MonkeyPatch, cors_headers: str) -> None:
    monkeypatch.setenv(
        "CORS_HEADERS",
        cors_headers,
    )
    settings = Settings()
    assert settings.cors_headers == [
        "Content-Type",
        "X-Foo",
    ]
