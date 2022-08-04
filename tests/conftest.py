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

    if request.node.get_closest_marker("with_token"):
        api_client.headers.update(
            {"Authorization": f"Bearer {jwt.create_access_token('some_user')}"}
        )


@pytest.fixture(scope="session")
def db_engine():
    import sqlalchemy as sqla

    from data_inclusion.api import settings
    from data_inclusion.api.core import db

    default_database_url = sqla.engine.make_url(settings.DATABASE_URL)

    default_db_engine = sqla.create_engine(
        default_database_url, isolation_level="AUTOCOMMIT"
    )

    # Connect to the db and creates a new test database
    with default_db_engine.connect() as default_db_conn:
        default_db_conn.execute(
            f"DROP DATABASE IF EXISTS {default_database_url.database}_test;"
        )
        default_db_conn.execute(f"CREATE DATABASE {default_database_url.database}_test")

    # Create a new connection pool for the test database
    test_db_engine = sqla.create_engine(f"{settings.DATABASE_URL}_test")

    # Create tables once
    db.init_db(engine=test_db_engine)

    yield test_db_engine

    # Teardown test database
    with default_db_engine.connect() as default_db_conn:
        default_db_conn.execute(
            f"DROP DATABASE IF EXISTS {default_database_url.database}_test WITH (FORCE);"
        )


@pytest.fixture(scope="function")
def db_session(db_engine):
    from data_inclusion.api.core import db

    connection = db_engine.connect()
    connection.begin()
    db_session = db.SessionLocal(bind=connection)
    yield db_session
    db_session.rollback()
    connection.close()
