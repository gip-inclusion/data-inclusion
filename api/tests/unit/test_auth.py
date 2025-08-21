import pytest

import fastapi
from fastapi.testclient import TestClient

from data_inclusion.api import auth


@pytest.mark.parametrize(
    ("token", "status_code"),
    [
        (None, 403),
        ("not-a-token", 403),
        pytest.param(
            # generated with key=legacy-key and sub=legacy.user@example.com
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJsZWdhY3kudXNlckBleGFtc"
            "GxlLmNvbSIsImFkbWluIjpmYWxzZX0.pqDa-_YHnPlsP0v0PWROb2Bgmvz7hUaYpjH_DPSb85Y",
            200,
        ),
        (auth.create_access_token("some_user"), 200),
    ],
)
def test_private_endpoint(token, status_code, monkeypatch):
    app = fastapi.FastAPI()

    # The auth module structure statically loads the settings in memory;
    # thus we have to monkeypatch the values in-place.
    from data_inclusion.api.auth.services import settings as auth_settings

    with monkeypatch.context() as m:
        m.setattr(auth_settings, "OLD_SECRET_KEY", "legacy-key")
        m.setattr(auth_settings, "OLD_USER_SUBS", ["legacy.user@example.com"])

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
