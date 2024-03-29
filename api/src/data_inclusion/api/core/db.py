import sqlalchemy as sqla
from sqlalchemy import orm

import fastapi

from data_inclusion.api.config import settings

default_db_engine = sqla.create_engine(settings.DATABASE_URL, pool_pre_ping=True)
SessionLocal = orm.sessionmaker(autoflush=False, bind=default_db_engine)


@orm.as_declarative()
class Base:
    __name__: str
    # Generate __tablename__ automatically

    @orm.declared_attr.directive
    def __tablename__(cls) -> str:
        return f"api_{cls.__name__.lower()}"


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
