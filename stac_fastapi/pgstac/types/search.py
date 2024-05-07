"""stac_fastapi.types.search module."""

from typing import Dict, Optional

from pydantic import ValidationInfo, field_validator
from stac_fastapi.types.search import BaseSearchPostRequest


class PgstacSearch(BaseSearchPostRequest):
    """Search model.

    Overrides the validation for datetime from the base request model.
    """

    conf: Optional[Dict] = None

    @field_validator("filter_lang", check_fields=False)
    @classmethod
    def validate_query_uses_cql(cls, v: str, info: ValidationInfo):
        """Use of Query Extension is not allowed with cql2."""
        if info.data.get("query", None) is not None and v != "cql-json":
            raise ValueError(
                "Query extension is not available when using pgstac with cql2"
            )

        return v
