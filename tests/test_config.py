"""test config."""

import warnings

import pytest
from pydantic import ValidationError

from stac_fastapi.pgstac.config import PostgresSettings


async def test_pg_settings_with_env(monkeypatch):
    """Test PostgresSettings with PG* environment variables"""
    monkeypatch.setenv("PGUSER", "username")
    monkeypatch.setenv("PGPASSWORD", "password")
    monkeypatch.setenv("PGHOST", "0.0.0.0")
    monkeypatch.setenv("PGPORT", "1111")
    monkeypatch.setenv("PGDATABASE", "pgstac")
    assert PostgresSettings(_env_file=None)


async def test_pg_settings_with_env_postgres(monkeypatch):
    """Test PostgresSettings with POSTGRES_* environment variables"""
    monkeypatch.setenv("POSTGRES_USER", "username")
    monkeypatch.setenv("POSTGRES_PASS", "password")
    monkeypatch.setenv("POSTGRES_HOST_READER", "0.0.0.0")
    monkeypatch.setenv("POSTGRES_HOST_WRITER", "0.0.0.0")
    monkeypatch.setenv("POSTGRES_PORT", "1111")
    monkeypatch.setenv("POSTGRES_DBNAME", "pgstac")
    with pytest.warns(DeprecationWarning) as record:
        assert PostgresSettings(_env_file=None)
    assert len(record) == 6


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

    # Compat, should work with old style postgres_ attributes
    # Should raise warnings on set attribute
    with pytest.warns(DeprecationWarning) as record:
        settings = PostgresSettings(
            postgres_user="user",
            postgres_pass="password",
            postgres_host_reader="0.0.0.0",
            postgres_port=1111,
            postgres_dbname="pgstac",
            _env_file=None,
        )
        assert settings.pghost == "0.0.0.0"
        assert len(record) == 5

    # Should raise warning when accessing deprecated attributes
    with pytest.warns(DeprecationWarning):
        assert settings.postgres_host_reader == "0.0.0.0"

    with pytest.raises(ValidationError):
        with pytest.warns(DeprecationWarning) as record:
            PostgresSettings(
                postgres_user="user",
                postgres_pass="password",
                postgres_host_reader="0.0.0.0",
                postgres_host_writer="1.1.1.1",
                postgres_port=1111,
                postgres_dbname="pgstac",
                _env_file=None,
            )
