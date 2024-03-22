import pytest

import fastapi
from fastapi.testclient import TestClient

from data_inclusion.api import auth


@pytest.mark.parametrize(
    ("token", "status_code"),
    [
        (None, 403),
        ("not-a-token", 403),
        (auth.create_access_token("some_user"), 200),
    ],
)
def test_private_endpoint(token, status_code):
    app = fastapi.FastAPI()

    @app.get("/api/private", dependencies=[auth.authenticated_dependency])
    def private_endpoint():
        pass

    api_client = TestClient(app)
    if token is not None:
        api_client.headers.update({"Authorization": f"Bearer {token}"})

    response = api_client.get("/api/private")
    assert response.status_code == status_code


@pytest.mark.parametrize(
    ("token", "status_code"),
    [
        (None, 403),
        ("not-a-token", 403),
        (auth.create_access_token("some_user"), 403),
        (auth.create_access_token("admin_user", admin=True), 200),
    ],
)
def test_admin_endpoint(token, status_code):
    app = fastapi.FastAPI()

    @app.get("/api/admin", dependencies=[auth.admin_dependency])
    def admin_endpoint():
        pass

    api_client = TestClient(app)
    if token is not None:
        api_client.headers.update({"Authorization": f"Bearer {token}"})

    response = api_client.get("/api/admin")
    assert response.status_code == status_code
