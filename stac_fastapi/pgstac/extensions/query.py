"""Pgstac query customisation."""

from enum import Enum
from typing import Any

from pydantic import BaseModel
from stac_fastapi.extensions.core.query import QueryExtension as QueryExtensionBase


class Operator(str, Enum):
    """Defines the set of operators supported by the API."""

    eq = "eq"
    ne = "ne"
    neq = "neq"
    lt = "lt"
    lte = "lte"
    gt = "gt"
    gte = "gte"


class QueryExtensionPostRequest(BaseModel):
    """Query Extension POST request model."""

    query: dict[str, dict[Operator, Any]] | None = None


class QueryExtension(QueryExtensionBase):
    """Query Extension.

    Override the POST request model to add validation against
    supported fields
    """

    POST = QueryExtensionPostRequest
