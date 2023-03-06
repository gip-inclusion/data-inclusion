import faker
import pytest


@pytest.fixture(autouse=True)
def seed():
    faker.Faker.seed(0)


@pytest.fixture()
def api_client(db_session):
    from fastapi.testclient import TestClient

    from data_inclusion.api.core import db
    from data_inclusion.api.entrypoints.fastapi import app

    app.dependency_overrides[db.get_session] = lambda: db_session

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
def test_database_url():
    import sqlalchemy as sqla

    from data_inclusion.api import settings

    default_database_url = sqla.engine.make_url(settings.DATABASE_URL)
    test_database_url = default_database_url.set(
        database=f"{default_database_url.database}_test"
    )

    default_db_engine = sqla.create_engine(
        default_database_url, isolation_level="AUTOCOMMIT"
    )

    # Connect to the db and creates a new test database
    with default_db_engine.connect() as default_db_conn:
        default_db_conn.execute(
            sqla.text(f'DROP DATABASE IF EXISTS "{test_database_url.database}";')
        )
        default_db_conn.execute(
            sqla.text(f'CREATE DATABASE "{test_database_url.database}";')
        )

    yield test_database_url

    # Teardown test database
    with default_db_engine.connect() as default_db_conn:
        default_db_conn.execute(
            sqla.text(
                f'DROP DATABASE IF EXISTS "{test_database_url.database}" WITH (FORCE);'
            )
        )


@pytest.fixture(scope="session")
def apply_db_migrations(test_database_url):
    from alembic import command
    from alembic.config import Config

    config = Config()
    config.set_main_option("script_location", "src/alembic/")
    config.set_main_option(
        "sqlalchemy.url", test_database_url.render_as_string(hide_password=False)
    )

    command.upgrade(config, "head")
    yield
    command.downgrade(config, "base")


@pytest.fixture(scope="session")
def db_engine(apply_db_migrations, test_database_url):
    import sqlalchemy as sqla

    # Create a new connection pool for the test database
    yield sqla.create_engine(test_database_url)


@pytest.fixture(scope="function")
def db_session(db_engine):
    from data_inclusion.api.core import db

    connection = db_engine.connect()
    connection.begin()
    db.SessionLocal.configure(bind=connection)
    db_session = db.SessionLocal()
    yield db_session
    db_session.rollback()
    connection.close()
