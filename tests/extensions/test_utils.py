from stac_fastapi.pgstac.extensions.utils import get_stac_api_extensions


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
