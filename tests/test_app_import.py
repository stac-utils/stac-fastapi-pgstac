"""Test that the main app can be imported and is properly configured."""

import sys

import pytest


@pytest.fixture(autouse=True)
def clean_app_module():
    """Ensure the app module is freshly imported for each toggle test, and cleaned up after."""
    # Remove from cache before test
    sys.modules.pop("stac_fastapi.pgstac.app", None)
    yield
    # Clean up after test so other files get a fresh import
    sys.modules.pop("stac_fastapi.pgstac.app", None)


def test_app_import():
    """Test that the main app can be imported without errors."""
    from stac_fastapi.pgstac.app import api, app

    assert api is not None
    assert app is not None


def test_app_has_required_routes():
    """Test that the app has the required STAC routes."""
    from stac_fastapi.pgstac.app import app

    # Safely extract paths or prefixes to support FastAPI >= 0.137 _IncludedRouter wrappers
    routes = set()
    for route in app.routes:
        if hasattr(route, "path"):
            routes.add(route.path)
        elif hasattr(route, "prefix"):  # _IncludedRouter or Mount
            routes.add(route.prefix)
        elif hasattr(route, "include_context"):  # Nested _IncludedRouter
            routes.add(route.include_context.prefix)

    # Check for core STAC routes
    assert any(r in ["/", ""] for r in routes)
    assert "/collections" in routes
    assert "/search" in routes
    assert "/conformance" in routes


def test_catalogs_extension_toggle(monkeypatch):
    """Test the app's behavior when toggling ENABLE_CATALOGS_EXTENSION."""

    # SCENARIO 1: Disabled
    monkeypatch.setenv("ENABLE_CATALOGS_EXTENSION", "false")
    import stac_fastapi.pgstac.app as pgstac_app_disabled

    # Ensure it booted, but catalogs extension isn't in the active extensions
    assert pgstac_app_disabled.api is not None
    active_ext_types = [type(ext) for ext in pgstac_app_disabled.application_extensions]

    # If the class exists in the namespace (i.e. installed), it still shouldn't be active
    if getattr(pgstac_app_disabled, "CatalogsExtension", None) is not None:
        assert pgstac_app_disabled.CatalogsExtension not in active_ext_types

    # Clear memory for Scenario 2 to force a fresh evaluation of app.py
    sys.modules.pop("stac_fastapi.pgstac.app", None)

    # SCENARIO 2: Enabled
    monkeypatch.setenv("ENABLE_CATALOGS_EXTENSION", "true")

    # Check if the optional dependency is actually installed in this Pytest environment
    import importlib.util

    has_ext = importlib.util.find_spec("stac_fastapi_catalogs_extension") is not None

    if not has_ext:
        # If not installed, it MUST raise our custom ImportError
        with pytest.raises(
            ImportError,
            match="ENABLE_CATALOGS_EXTENSION is set to true, but the catalogs extension is not installed",
        ):
            import stac_fastapi.pgstac.app as pgstac_app_enabled  # noqa: F401
    else:
        # If installed, it must boot and successfully register ALL catalog extensions
        import stac_fastapi.pgstac.app as pgstac_app_enabled

        active_ext_types = [
            type(ext) for ext in pgstac_app_enabled.application_extensions
        ]

        assert pgstac_app_enabled.CatalogsExtension in active_ext_types
