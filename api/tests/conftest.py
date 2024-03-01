from pathlib import Path

import faker
import pytest
import sqlalchemy as sqla
from alembic import command
from alembic.config import Config

from fastapi.testclient import TestClient

from data_inclusion.api import settings
from data_inclusion.api.core import db
from data_inclusion.api.core.request.services import db as middleware_db
from data_inclusion.api.entrypoints.fastapi import app

from . import factories

DEFAULT_DATABASE_URL = sqla.engine.make_url(settings.DATABASE_URL)
TEST_DATABASE_URL = DEFAULT_DATABASE_URL.set(
    database=f"{DEFAULT_DATABASE_URL.database}_test"
)


@pytest.fixture()
def api_client(db_init, test_session):
    middleware_db.SessionLocal = factories.TestSession
    app.dependency_overrides[db.get_session] = lambda: test_session

    with TestClient(app) as c:
        yield c


@pytest.fixture(autouse=True)
def force_authenticate(request, api_client):
    """Automatically authenticate generated requests.

    Inject a valid token header to the test client, when tests are flagged with the
    `with_token` mark.
    """
    from data_inclusion.api.core import jwt

    token = None

    if request.node.get_closest_marker("with_token"):
        token = jwt.create_access_token("some_user")
    elif request.node.get_closest_marker("with_admin_token"):
        token = jwt.create_access_token("admin_user", admin=True)

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
        conn.commit()

    # Migrate the database
    config = Config()
    config.set_main_option("script_location", "src/alembic/")
    config.set_main_option(
        "sqlalchemy.url", TEST_DATABASE_URL.render_as_string(hide_password=False)
    )

    command.upgrade(config, "head")

    factories.TestSession.configure(bind=test_engine)

    yield test_engine

    factories.TestSession.remove()

    with default_engine.connect() as conn:
        conn.execute(
            sqla.text(
                f'DROP DATABASE IF EXISTS "{TEST_DATABASE_URL.database}" WITH (FORCE)'
            )
        )


@pytest.fixture(scope="session")
def db_engine(db_init):
    yield db_init


@pytest.fixture(scope="session")
def generate_communes_nord(db_engine):
    import geopandas

    df = geopandas.read_file(Path(__file__).parent / "communes_nord.sqlite")
    df = df.rename_geometry("geom")

    with db_engine.connect() as conn:
        df.to_postgis(
            "admin_express_communes",
            con=conn,
            if_exists="replace",
            index=False,
        )


@pytest.fixture()
def test_session(db_init):
    faker.Faker.seed(0)
    with factories.TestSession() as session:
        yield session


@pytest.fixture(autouse=True)
def predictable_sequences():
    import factory.random

    factory.random.reseed_random(0)
    factories.RequestFactory.reset_sequence()
    factories.CommuneFactory.reset_sequence()
    factories.ServiceFactory.reset_sequence()
    factories.SourceFactory.reset_sequence()
    factories.StructureFactory.reset_sequence()


@pytest.fixture(autouse=True)
def create_default_sources(test_session):
    factories.SourceFactory(slug="dora")
    factories.SourceFactory(slug="emplois-de-linclusion")
