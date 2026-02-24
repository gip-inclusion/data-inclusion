from unittest.mock import ANY

import jwt
import pytest

from data_inclusion.api import auth
from data_inclusion.api.config import settings

LEGACY_WIDGET_TOKEN = jwt.encode(
    payload={
        "sub": "legacy-widget@example.com",
        "allowed_origins": ["example.com"],
    },
    key=settings.SECRET_KEY,
    algorithm="HS256",
)
LEGACY_API_TOKEN = jwt.encode(
    payload={
        "sub": "legacy.user@example.com",
        "admin": False,
    },
    key=settings.SECRET_KEY,
    algorithm="HS256",
)
LEGACY_ADMIN_TOKEN = jwt.encode(
    payload={
        "sub": "legacy-admin@example.com",
        "admin": True,
    },
    key=settings.SECRET_KEY,
    algorithm="HS256",
)


def create_token_with_admin(api_client, payload: dict) -> str:
    response = api_client.post(
        "/auth/create_token/",
        json=payload,
        headers={"Authorization": f"Bearer {LEGACY_ADMIN_TOKEN}"},
    )
    assert response.status_code == 200
    return response.json()["access"]


def test_create_token_unauthenticated(api_client):
    response = api_client.post(
        "/auth/create_token/",
        json={"email": "foo@bar.com"},
    )
    assert response.status_code == 401


@pytest.mark.with_token
def test_create_token_unauthorized(api_client):
    response = api_client.post(
        "/auth/create_token/",
        json={"email": "foo@bar.com"},
    )
    assert response.status_code == 403


@pytest.mark.with_token(scopes=["admin"])
@pytest.mark.parametrize(
    ("payload", "expected_hosts", "expected_token_payload"),
    [
        (
            {"email": "widget@example.com", "hosts": ["https://example.com"]},
            ["example.com"],
            {
                "sub": "widget@example.com",
                "scopes": ["widget"],
                "created_at": ANY,
                "allowed_hosts": ["example.com"],
            },
        ),
        (
            {"email": "widget@example.com", "hosts": ["sub.example.com"]},
            ["sub.example.com"],
            {
                "sub": "widget@example.com",
                "scopes": ["widget"],
                "created_at": ANY,
                "allowed_hosts": ["sub.example.com"],
            },
        ),
    ],
)
def test_create_token_with_hosts(
    api_client, payload, expected_hosts, expected_token_payload
):
    response = api_client.post("/auth/create_token/", json=payload)

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data == {"access": ANY, "allowed_hosts": expected_hosts}
    assert auth.verify_token(token=resp_data["access"]) == expected_token_payload


def test_robots(api_client):
    response = api_client.get("/robots.txt")
    assert response.content == b"User-agent: *\nAllow: /api/v0/docs\nDisallow: /\n"


@pytest.mark.parametrize(
    ("headers", "status_code"),
    [
        ({}, 401),
        ({"Authorization": "Bearer not-a-token"}, 401),
        ({"Authorization": f"Bearer {LEGACY_API_TOKEN}"}, 200),
        ({"Authorization": f"Bearer {LEGACY_ADMIN_TOKEN}"}, 200),
        ({"Authorization": f"Bearer {LEGACY_WIDGET_TOKEN}"}, 403),
    ],
)
def test_api_endpoints_auth_legacy_tokens(api_client, headers, status_code):
    response = api_client.get("/api/v1/search/services", headers=headers)
    assert response.status_code == status_code


@pytest.mark.parametrize(
    ("payload", "status_code"),
    [
        ({"email": "api@example.com", "scopes": ["api"]}, 200),
        (
            {
                "email": "widget@example.com",
                "scopes": ["widget"],
                "hosts": ["*"],
            },
            403,
        ),
    ],
)
def test_api_endpoints_auth(api_client, payload, status_code):
    token = create_token_with_admin(
        api_client,
        payload=payload,
    )
    headers = {"Authorization": f"Bearer {token}"}
    response = api_client.get("/api/v1/search/services", headers=headers)
    assert response.status_code == status_code


@pytest.mark.parametrize(
    ("params", "status_code"),
    [
        ({}, 422),
        ({"token": "not-a-token"}, 403),
        ({"token": LEGACY_WIDGET_TOKEN}, 200),
    ],
)
def test_widget_endpoint_auth_legacy_tokens(api_client, params, status_code):
    response = api_client.get("/widget/", params=params)
    assert response.status_code == status_code


@pytest.mark.parametrize(
    ("payload", "status_code"),
    [
        ({"email": "api@example.com", "scopes": ["api"]}, 403),
        (
            {
                "email": "widget@example.com",
                "scopes": ["widget"],
                "hosts": ["*"],
            },
            200,
        ),
    ],
)
def test_widget_endpoint_auth(api_client, payload, status_code):
    token = create_token_with_admin(
        api_client,
        payload=payload,
    )
    params = {"token": token}
    response = api_client.get("/widget/", params=params)
    assert response.status_code == status_code
