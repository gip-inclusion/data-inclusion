from typing import Generic, TypeVar

import fastapi
import fastapi_pagination

from data_inclusion.api import settings

T = TypeVar("T")


class Params(fastapi_pagination.Params):
    size: int = fastapi.Query(
        settings.DEFAULT_PAGE_SIZE,
        ge=1,
        le=settings.MAX_PAGE_SIZE,
        description="Page size",
    )


class Page(fastapi_pagination.Page[T], Generic[T]):
    __params_type__ = Params
