from typing import TypeVar

import fastapi
from fastapi_pagination import Page
from fastapi_pagination.customization import (
    CustomizedPage,
    UseName,
    UseParamsFields,
)

from data_inclusion.api.config import settings

T = TypeVar("T")

BigPage = CustomizedPage[
    Page[T],
    UseName("CustomizedPage"),
    UseParamsFields(
        size=fastapi.Query(
            default=settings.DEFAULT_PAGE_SIZE,
            ge=1,
            le=settings.MAX_PAGE_SIZE,
            description="Page size",
        )
    ),
]
