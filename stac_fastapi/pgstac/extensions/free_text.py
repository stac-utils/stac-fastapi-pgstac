"""Free-Text model for PgSTAC."""

from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic.functional_serializers import PlainSerializer
from stac_fastapi.extensions.core.free_text import (
    FreeTextExtension as FreeTextExtensionBase,
)
from typing_extensions import Annotated


class FreeTextExtensionPostRequest(BaseModel):
    """Free-text Extension POST request model."""

    q: Annotated[
        Optional[List[str]],
        PlainSerializer(lambda x: " OR ".join(x), return_type=str, when_used="json"),
    ] = Field(
        None,
        description="Parameter to perform free-text queries against STAC metadata",
    )


class FreeTextExtension(FreeTextExtensionBase):
    """FreeText Extension.

    Override the POST request model to add custom serialization
    """

    POST = FreeTextExtensionPostRequest
