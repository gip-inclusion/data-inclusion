from typing import Annotated

import pydantic
import pytest
import sqlalchemy as sa
from sqlalchemy import orm, pool

import fastapi
from fastapi.testclient import TestClient

from data_inclusion.api.config import settings
from data_inclusion.api.utils import pagination


class Base(orm.DeclarativeBase):
    pass


class Item(Base):
    __tablename__ = "items"
    id: orm.Mapped[Annotated[int, orm.mapped_column(primary_key=True)]]


@pytest.fixture
def session():
    engine = sa.create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=pool.StaticPool,
    )
    Base.metadata.create_all(bind=engine)

    with orm.Session(bind=engine) as session:
        yield session


class Undefined:
    pass


@pytest.mark.parametrize(
    (
        "items_count",
        "input_page",
        "input_size",
        "expected_page",
        "expected_size",
        "expected_pages",
    ),
    [
        # First page
        (1000, 1, 10, 1, 10, 100),
        # Second page
        (1000, 2, 10, 2, 10, 100),
        # Default page is 1
        (1000, Undefined, 10, 1, 10, 100),
        # Size larger than total items
        (100, 1, 1000, 1, 100, 1),
        # Default size from config
        (
            settings.DEFAULT_PAGE_SIZE + 1,
            1,
            Undefined,
            1,
            settings.DEFAULT_PAGE_SIZE,
            2,
        ),
    ],
)
def test_page_size_pagination(
    session: orm.Session,
    items_count: int,
    input_page: int | Undefined,
    input_size: int | Undefined,
    expected_page: int,
    expected_size: int,
    expected_pages: int,
):
    app = fastapi.FastAPI()

    class ItemOut(pydantic.BaseModel):
        id: int

    class QueryParams(pydantic.BaseModel, pagination.get_pagination_params()):
        pass

    @app.get("/items/", response_model=pagination.Page[ItemOut])
    def list_items(params: Annotated[QueryParams, fastapi.Query()]):
        return pagination.paginate(
            session,
            sa.select(Item),
            page=params.page,
            size=params.size,
        )

    session.add_all(Item(id=i) for i in range(items_count))
    session.commit()

    with TestClient(app) as client:
        params = {}
        if input_page is not Undefined:
            params["page"] = input_page
        if input_size is not Undefined:
            params["size"] = input_size

        response = client.get("/items/", params=params)

        assert response.status_code == 200
        assert response.json() == {
            "items": [
                {"id": i}
                for i in range(
                    (expected_page - 1) * expected_size,
                    expected_page * expected_size,
                )
            ],
            "page": expected_page,
            "size": expected_size,
            "total": items_count,
            "pages": expected_pages,
        }
