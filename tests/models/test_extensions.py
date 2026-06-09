import pytest
from stac_fastapi.types.extension import ApiExtension

from stac_fastapi.pgstac.models.extensions import (
    DEFAULT_EXTENSIONS,
    Extensions,
    get_stac_api_extensions,
)


def test_get_stac_api_extensions():
    """Test that get_stac_api_extensions returns the correct extensions."""
    default_extensions = {
        "query": "default_query_extension",
        "filter": "default_filter_extension",
    }
    update_extensions = {
        "query": "updated_query_extension",
        "new_extension": "new_extension",
    }

    extensions = get_stac_api_extensions(
        default=default_extensions, update=update_extensions
    )

    assert extensions["query"] == "updated_query_extension"
    assert extensions["filter"] == "default_filter_extension"
    assert extensions["new_extension"] == "new_extension"


def test_get_stac_api_extensions_no_update():
    """Test that get_stac_api_extensions returns the default extensions when no update is provided."""
    default_extensions = {
        "query": "default_query_extension",
        "filter": "default_filter_extension",
    }

    extensions = get_stac_api_extensions(default=default_extensions)

    assert extensions == default_extensions


def test_extensions_type_error():
    """Test that a TypeError is raised when non-ApiExtension values are provided."""
    with pytest.raises(TypeError) as exc_info:
        Extensions(search={"query": "not_an_extension"})
    assert "contains values that are not ApiExtension instances" in str(exc_info.value)


def test_extensions_default_enabled():
    """Test that default_enabled returns the correct set of enabled extensions."""

    class CustomNewExtension(ApiExtension):
        def register(self, app) -> None:
            pass

    extensions = Extensions(
        search={"new_extension": CustomNewExtension()},
    )
    expected_enabled = set(
        list(DEFAULT_EXTENSIONS["search"].keys())
        + list(DEFAULT_EXTENSIONS["collection_search"].keys())
        + list(DEFAULT_EXTENSIONS["item_collection"].keys())
        + ["collection_search", "new_extension"]
    )
    assert extensions.default_enabled == expected_enabled
