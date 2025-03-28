import json
from pathlib import Path

import faker
import pytest
import sqlalchemy as sqla
from alembic import command
from alembic.config import Config

from fastapi.testclient import TestClient

from data_inclusion.api.app import create_app
from data_inclusion.api.config import settings
from data_inclusion.api.core import db
from data_inclusion.api.decoupage_administratif.models import Commune

from . import factories

DEFAULT_DATABASE_URL = sqla.engine.make_url(settings.DATABASE_URL)
TEST_DATABASE_URL = DEFAULT_DATABASE_URL.set(
    database=f"{DEFAULT_DATABASE_URL.database}_test"
)

DIR = Path(__file__).parent


@pytest.fixture(scope="session")
def app():
    settings.BASE_URL = "http://testserver"
    settings.ENV = "test"
    yield create_app()


def swap_middleware(app, before, after):
    for m in app.user_middleware:
        if m.kwargs.get("dispatch") == before:
            m.kwargs["dispatch"] = after


@pytest.fixture(scope="function")
def api_client(app, db_session):
    async def db_session_middleware(request, call_next):
        request.state.db_session = db_session
        return await call_next(request)

    # swapping middleware is a lot faster than recreating the app
    # with the middleware as a parameter
    swap_middleware(app, db.db_session_middleware, db_session_middleware)
    app.dependency_overrides[db.get_session] = lambda: db_session

    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="function", autouse=True)
def force_authenticate(request, api_client):
    """Automatically authenticate generated requests.

    Inject a valid token header to the test client, when tests are flagged with the
    `with_token` mark.
    """
    from data_inclusion.api import auth

    token = None

    if request.node.get_closest_marker("with_token"):
        token = auth.create_access_token("some_user")
    elif request.node.get_closest_marker("with_admin_token"):
        token = auth.create_access_token("admin_user", admin=True)
    elif request.node.get_closest_marker("with_dora_token"):
        token = auth.create_access_token("dora")

    if token is not None:
        api_client.headers.update({"Authorization": f"Bearer {token}"})


@pytest.fixture(scope="session")
def db_init():
    default_engine = sqla.create_engine(
        DEFAULT_DATABASE_URL, isolation_level="AUTOCOMMIT"
    )
    test_engine = sqla.create_engine(TEST_DATABASE_URL)

    # Connect to the db and creates a new test database
    with default_engine.connect() as conn:
        conn.execute(
            sqla.text(f'DROP DATABASE IF EXISTS "{TEST_DATABASE_URL.database}"')
        )
        conn.execute(sqla.text(f'CREATE DATABASE "{TEST_DATABASE_URL.database}"'))

    # Create postgis extension in test database
    with test_engine.connect() as conn:
        conn.execute(sqla.text("CREATE EXTENSION postgis;"))
        conn.execute(sqla.text("CREATE EXTENSION unaccent;"))
        conn.commit()

    # Migrate the database
    config = Config()
    config.set_main_option("script_location", "src/alembic/")
    config.set_main_option(
        "sqlalchemy.url", TEST_DATABASE_URL.render_as_string(hide_password=False)
    )

    command.upgrade(config, "head")

    yield test_engine

    with default_engine.connect() as conn:
        conn.execute(
            sqla.text(
                f'DROP DATABASE IF EXISTS "{TEST_DATABASE_URL.database}" WITH (FORCE)'
            )
        )


@pytest.fixture(scope="session")
def db_engine(db_init):
    yield db_init


@pytest.fixture(scope="session", autouse=True)
def communes(db_connection):
    # handpicked data from :
    # https://geo.api.gouv.fr/decoupage-administratif/communes
    commune_data_list = json.load((DIR / "communes.json").open())

    db_connection.execute(sqla.insert(Commune).values(commune_data_list))
    db_connection.commit()


@pytest.fixture(scope="session")
def db_connection(db_init):
    connnection = db_init.connect()
    yield connnection
    connnection.close()


@pytest.fixture(scope="function")
def db_session(db_connection):
    faker.Faker.seed(0)

    # https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#joining-a-session-into-an-external-transaction-such-as-for-test-suites  # noqa
    transaction = db_connection.begin()
    session = sqla.orm.Session(
        bind=db_connection, join_transaction_mode="create_savepoint"
    )

    factories.StructureFactory._meta.sqlalchemy_session = session
    factories.ServiceFactory._meta.sqlalchemy_session = session

    yield session

    session.close()
    transaction.rollback()


@pytest.fixture(autouse=True)
def predictable_sequences():
    import factory.random

    factory.random.reseed_random(0)
    factories.ServiceFactory.reset_sequence()
    factories.StructureFactory.reset_sequence()
