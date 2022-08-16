import sqlalchemy as sqla
from sqlalchemy import orm
from sqlalchemy.ext.declarative import as_declarative, declared_attr

from data_inclusion.api import settings

default_db_engine = sqla.create_engine(settings.DATABASE_URL, pool_pre_ping=True)
SessionLocal = orm.sessionmaker(autoflush=False, bind=default_db_engine)


@as_declarative()
class Base:
    __name__: str
    # Generate __tablename__ automatically

    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__.lower()


def init_db(engine=default_db_engine):
    Base.metadata.create_all(bind=engine)


def get_session():
    with SessionLocal() as session:
        yield session
