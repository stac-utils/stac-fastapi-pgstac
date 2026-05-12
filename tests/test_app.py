from stac_fastapi.pgstac.app import instantiate_api
from stac_fastapi.pgstac.core import CoreCrudClient


def test_instantiate_api_custom_client():
    """Test that a custom client class is used when passed to instantiate_api."""

    class CustomClient(CoreCrudClient):
        pass

    api = instantiate_api(client=CustomClient)

    assert isinstance(api.client, CustomClient)
