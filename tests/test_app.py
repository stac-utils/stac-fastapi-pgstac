from unittest.mock import patch

import pytest
from stac_fastapi.types.extension import ApiExtension

from stac_fastapi.pgstac.app import (
    CollectionSearchExtension,
    StacApi,
    create_get_request_model,
    create_post_request_model,
    create_request_model,
    instantiate_api,
)
from stac_fastapi.pgstac.config import Settings
from stac_fastapi.pgstac.core import CoreCrudClient
from stac_fastapi.pgstac.models.extensions import DEFAULT_EXTENSIONS, Extensions


class TestApiExtension(ApiExtension):
    def register(self, app) -> None:
        pass


class CustomQueryExtension(TestApiExtension):
    pass


class CustomNewExtension(TestApiExtension):
    pass


class DefaultQueryExtension(TestApiExtension):
    pass


class DefaultSortExtension(TestApiExtension):
    pass


def test_instantiate_app_settings_type_error():
    with pytest.raises(TypeError) as exc_info:
        instantiate_api(settings="not_a_settings_instance")
    assert "Expected `settings` to be an instance of `Settings`" in str(exc_info.value)


def test_instantiate_app_client_type_error():
    with pytest.raises(TypeError) as exc_info:
        instantiate_api(client="not_a_client_class")
    assert "Expected `client` to be a subclass of `CoreCrudClient`" in str(exc_info.value)


def test_instantiate_app_extensions_type_error():
    with pytest.raises(TypeError) as exc_info:
        instantiate_api(extensions="not_an_extensions_instance")
    assert "Expected `extensions` to be an instance of `Extensions`" in str(
        exc_info.value
    )


def test_instantiate_api_default_settings():
    """Test that the default settings are used when no settings are passed to instantiate_api."""
    api = instantiate_api()

    assert api.settings == Settings()


def test_instantiate_api_custom_settings():
    """Test that custom settings are used when passed to instantiate_api."""

    class CustomSettings(Settings):
        custom_setting: str = "custom_value"

    custom_settings = CustomSettings()
    api = instantiate_api(settings=custom_settings)

    assert api.settings == custom_settings


def test_instantiate_api_default_client():
    """Test that the default client class is used when no client is passed to instantiate_api."""
    api = instantiate_api()

    assert isinstance(api.client, CoreCrudClient)


def test_instantiate_api_custom_client():
    """Test that a custom client class is used when passed to instantiate_api."""

    class CustomClient(CoreCrudClient):
        pass

    api = instantiate_api(client=CustomClient)

    assert isinstance(api.client, CustomClient)


def test_instantiate_api_default_search_extensions():
    """Test that the default search extensions are used when no search_extensions_update is passed to instantiate_api."""
    with (
        patch(
            "stac_fastapi.pgstac.app.create_post_request_model",
            wraps=create_post_request_model,
        ) as mock_post,
        patch(
            "stac_fastapi.pgstac.app.create_get_request_model",
            wraps=create_get_request_model,
        ) as mock_get,
    ):
        instantiate_api()

        extensions_post = mock_post.call_args.args[0]
        extensions_get = mock_get.call_args.args[0]

    assert extensions_post == extensions_get
    default_extensions = DEFAULT_EXTENSIONS.get("search", {}).values()
    assert len(extensions_post) == len(list(default_extensions))
    exp_extension_classes = [type(ext) for ext in default_extensions]
    for ext in default_extensions:
        assert type(ext) in exp_extension_classes


def test_instantiate_api_custom_search_extensions():
    """Test that custom search extensions are used when passed to instantiate_api."""
    custom_extensions = {
        "query": CustomQueryExtension(),
        "new_extension": CustomNewExtension(),
    }

    with (
        patch(
            "stac_fastapi.pgstac.app.create_post_request_model",
            wraps=create_post_request_model,
        ) as mock_post,
        patch(
            "stac_fastapi.pgstac.app.create_get_request_model",
            wraps=create_get_request_model,
        ) as mock_get,
    ):
        instantiate_api(extensions=Extensions(search=custom_extensions))
        extensions_post = mock_post.call_args.args[0]
        extensions_get = mock_get.call_args.args[0]

    assert extensions_post == extensions_get
    assert len(extensions_post) == len(
        set(DEFAULT_EXTENSIONS.get("search", {}).keys()).union(custom_extensions.keys())
    )
    for type_ext in [type(ext) for ext in custom_extensions.values()]:
        assert type_ext in [type(e) for e in extensions_post]
    for type_ext in [type(ext) for ext in DEFAULT_EXTENSIONS.get("search", {}).values()]:
        assert type_ext in [type(e) for e in extensions_post]


def test_instantiate_api_default_collection_search_extensions():
    """Test that the default collection search extensions are used when no collection_search_extensions_update is passed to instantiate_api."""
    with patch(
        "stac_fastapi.pgstac.app.CollectionSearchExtension.from_extensions",
        wraps=CollectionSearchExtension.from_extensions,
    ) as mock_extensions:
        instantiate_api()
        extensions = mock_extensions.call_args.args[0]

    default_extensions = DEFAULT_EXTENSIONS.get("collection_search", {}).values()
    assert len(extensions) == len(list(default_extensions))
    exp_extension_classes = [type(ext) for ext in default_extensions]
    for ext in default_extensions:
        assert type(ext) in exp_extension_classes


def test_instantiate_api_custom_collection_search_extensions():
    """Test that custom collection search extensions are used when passed to instantiate_api."""
    custom_extensions = {
        "query": CustomQueryExtension(),
        "new_extension": CustomNewExtension(),
    }

    with patch(
        "stac_fastapi.pgstac.app.CollectionSearchExtension.from_extensions",
        wraps=CollectionSearchExtension.from_extensions,
    ) as mock_extensions:
        instantiate_api(extensions=Extensions(collection_search=custom_extensions))
        extensions = mock_extensions.call_args.args[0]

    assert len(extensions) == len(
        set(DEFAULT_EXTENSIONS.get("collection_search", {}).keys()).union(
            custom_extensions.keys()
        )
    )
    for type_ext in [type(ext) for ext in custom_extensions.values()]:
        assert type_ext in [type(e) for e in extensions]
    for type_ext in [
        type(ext) for ext in DEFAULT_EXTENSIONS.get("collection_search", {}).values()
    ]:
        assert type_ext in [type(e) for e in extensions]


def test_intantiate_api_default_item_collection_extensions():
    """Test that the default item collection extensions are used when no item_collection_extensions_update is passed to instantiate_api."""
    with patch(
        "stac_fastapi.pgstac.app.create_request_model", wraps=create_request_model
    ) as mock_request_model:
        instantiate_api()
        extensions = mock_request_model.call_args.kwargs["extensions"]

    default_extensions = DEFAULT_EXTENSIONS.get("item_collection", {}).values()
    assert len(extensions) == len(list(default_extensions))
    exp_extension_classes = [type(ext) for ext in default_extensions]
    for ext in default_extensions:
        assert type(ext) in exp_extension_classes


def test_intantiate_api_custom_item_collection_extensions():
    """Test that custom item collection extensions are used when passed to instantiate_api."""
    custom_extensions = {
        "query": CustomQueryExtension(),
        "new_extension": CustomNewExtension(),
    }

    with (
        patch(
            "stac_fastapi.pgstac.app.create_request_model", wraps=create_request_model
        ) as mock_request_model,
    ):
        instantiate_api(extensions=Extensions(item_collection=custom_extensions))
        extensions = mock_request_model.call_args.kwargs["extensions"]

    assert len(extensions) == len(
        set(DEFAULT_EXTENSIONS.get("item_collection", {}).keys()).union(
            custom_extensions.keys()
        )
    )
    for type_ext in [type(ext) for ext in custom_extensions.values()]:
        assert type_ext in [type(e) for e in extensions]
    for type_ext in [
        type(ext) for ext in DEFAULT_EXTENSIONS.get("item_collection", {}).values()
    ]:
        assert type_ext in [type(e) for e in extensions]


def test_instantiate_api_extra_extensions():
    """Test that extra extensions are used when passed to instantiate_api."""
    extra_extensions = {
        "extra_extension": CustomNewExtension(),
    }

    with patch("stac_fastapi.pgstac.app.StacApi", wraps=StacApi) as mock_stac_api:
        instantiate_api(extensions=Extensions(extra=extra_extensions))
        extensions = mock_stac_api.call_args.kwargs["extensions"]

    for type_ext in [type(ext) for ext in extra_extensions.values()]:
        assert type_ext in [type(e) for e in extensions]
