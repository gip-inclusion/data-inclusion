import fastapi
import fastapi_pagination

from data_inclusion.api import settings

Page = fastapi_pagination.Page.with_custom_options(
    size=fastapi.Query(
        default=settings.DEFAULT_PAGE_SIZE,
        ge=1,
        le=settings.MAX_PAGE_SIZE,
        description="Page size",
    ),
)
