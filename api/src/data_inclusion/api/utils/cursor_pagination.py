from typing import Generic, TypeVar

from pydantic import BaseModel, Field
from sqlalchemy import Select
from sqlalchemy.sql import text

import fastapi

T = TypeVar("T")


class CursorPage(BaseModel, Generic[T]):
    items: list[T]
    next_cursor: str | None = None
    previous_cursor: str | None = None
    total: int = Field(description="Total number of items across all pages")


class CursorParams:
    def __init__(
        self,
        cursor: str | None = fastapi.Query(None, description="Cursor for pagination"),
        limit: int = fastapi.Query(
            5000, ge=1, le=10000, description="Number of items per page"
        ),
    ):
        self.cursor = cursor
        self.limit = limit


def apply_cursor_pagination(
    query: Select, cursor_params: CursorParams, cursor_column: str
) -> Select:
    """Apply cursor-based pagination to a SQLAlchemy select query.

    Args:
        query: The base SQLAlchemy select query
        cursor_params: The cursor parameters (cursor token and limit)
        cursor_column: The name of the column to use for cursor-based pagination

    Returns:
        The modified query with cursor-based pagination applied
    """
    if cursor_params.cursor:
        # Decode the cursor (in this case, it's the last seen ID)
        try:
            last_seen_value = cursor_params.cursor
            query = query.where(text(f"{cursor_column} > :cursor")).params(
                cursor=last_seen_value
            )
        except ValueError:
            pass

    # Add 1 extra item to check if there are more pages
    query = query.limit(cursor_params.limit + 1)

    return query


def get_cursor_response[T](
    items: list[T],
    cursor_params: CursorParams,
    cursor_column: str,
    total: int,
) -> CursorPage[T]:
    """Create a cursor pagination response.

    Args:
        items: The list of items for the current page
        cursor_params: The cursor parameters used in the query
        cursor_column: The name of the column used for cursor-based pagination
        total: The total count of items across all pages

    Returns:
        A CursorPage instance with items and pagination metadata
    """
    # Check if we fetched an extra item indicating more pages exist
    has_next = len(items) > cursor_params.limit
    if has_next:
        items = items[:-1]  # Remove the extra item

    # Get the next cursor if there are more items
    next_cursor = None
    if has_next and items:
        next_cursor = str(getattr(items[-1], cursor_column))

    # Previous cursor implementation could be added here if needed
    previous_cursor = cursor_params.cursor

    return CursorPage(
        items=items,
        next_cursor=next_cursor,
        previous_cursor=previous_cursor,
        total=total,
    )
