import re
import uuid
from datetime import datetime
from typing import Annotated

import sqlalchemy as sqla
import sqlalchemy.types as types
from sqlalchemy import orm
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import mapped_column

import fastapi

from data_inclusion.api.config import settings

default_db_engine = sqla.create_engine(settings.DATABASE_URL, pool_pre_ping=True)
SessionLocal = orm.sessionmaker(autoflush=False, bind=default_db_engine)

uuid_pk = Annotated[uuid.UUID, mapped_column(primary_key=True, default=uuid.uuid4)]
timestamp = Annotated[
    datetime,
    mapped_column(sqla.DateTime(timezone=True), server_default=sqla.func.now()),
]


class SortedTextArray(types.TypeDecorator):
    """Automatically sorts a list of strings when inserting into the database."""

    impl = ARRAY(sqla.Text)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        filtered = [v for v in value if v is not None]
        return sorted(filtered) if filtered else []


class Base(orm.DeclarativeBase):
    metadata = sqla.MetaData(
        naming_convention={
            "ix": "ix_%(table_name)s__%(column_0_N_name)s",
            "uq": "uq_%(table_name)s__%(column_0_N_name)s",
            "ck": "ck_%(table_name)s__%(constraint_name)s",
            "fk": "fk_%(table_name)s__%(column_0_N_name)s__%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )
    type_annotation_map = {
        list[str]: SortedTextArray,
        list[dict]: JSONB,
        dict: JSONB,
    }

    __name__: str
    # Generate __tablename__ automatically

    @orm.declared_attr.directive
    def __tablename__(cls) -> str:
        snake_case_name = re.sub(r"(?<!^)(?=[A-Z])", "_", cls.__name__).lower()
        return f"api__{snake_case_name}s"


def get_session(request: fastapi.Request):
    yield request.state.db_session


async def db_session_middleware(request: fastapi.Request, call_next):
    response = fastapi.Response("Internal server error", status_code=500)
    try:
        request.state.db_session = SessionLocal()
        response = await call_next(request)
    finally:
        request.state.db_session.close()
    return response
