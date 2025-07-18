from math import ceil
from typing import Annotated

import pydantic
import sqlalchemy as sa
from sqlalchemy import orm

from data_inclusion.api.config import settings


class PaginationParamsMixin:
    page: Annotated[int, pydantic.Field(ge=1, description="Page number")] = 1
    size: Annotated[
        int,
        pydantic.Field(
            ge=1,
            le=settings.MAX_PAGE_SIZE,
            description="Page size",
        ),
    ] = settings.DEFAULT_PAGE_SIZE


class Page[T](pydantic.BaseModel):
    items: list[T]
    total: int
    page: int
    size: int
    pages: int


def paginate(
    db_session: orm.Session,
    query: sa.Select,
    size: int,
    page: int,
    mapping: tuple | None = None,
) -> dict:
    limit, offset = size, size * (page - 1)

    count_query = sa.select(sa.func.count()).select_from(
        query.order_by(None).options(orm.noload("*")).subquery()
    )

    query = query.limit(limit).offset(offset)

    if mapping is not None:
        items = db_session.execute(query).all()
        items = [dict(zip(mapping, item, strict=True)) for item in items]
    else:
        items = db_session.execute(query).scalars().all()

    total = db_session.execute(count_query).scalar()

    if size == 0:
        pages = 0
    elif total is not None:
        pages = ceil(total / size)
    else:
        pages = None

    return {
        "items": items,
        "total": total,
        "page": page,
        "size": len(items),
        "pages": pages,
    }
