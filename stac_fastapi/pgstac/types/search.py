"""stac_fastapi.types.search module."""

from pydantic import ValidationInfo, field_validator, model_validator
from stac_fastapi.types.search import BaseSearchPostRequest

from stac_fastapi.pgstac.utils import clean_exclude_set


class PgstacSearch(BaseSearchPostRequest):
    """Search model.

    Overrides the validation for datetime from the base request model.
    """

    conf: dict | None = None

    @field_validator("filter_lang", check_fields=False)
    @classmethod
    def validate_query_uses_cql(cls, v: str, info: ValidationInfo):
        """Use of Query Extension is not allowed with cql2."""
        if info.data.get("query", None) is not None:
            raise ValueError(
                "Query extension is not available when using pgstac with cql2"
            )

        return v

    @model_validator(mode="after")
    def validate_fields(self):
        """Clean the exclude set to give the include set precedence."""
        fields = getattr(self, "fields", None)
        if fields and fields.include and fields.exclude:
            fields.exclude = clean_exclude_set(fields.exclude, fields.include)
        return self
