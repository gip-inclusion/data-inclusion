from unittest.mock import ANY

import pytest

from data_inclusion.api import auth


def test_create_token_unauthenticated(api_client):
    response = api_client.post(
        "/api/v0/create_token/",
        json={"email": "foo@bar.com"},
    )
    assert response.status_code == 401


@pytest.mark.with_token
def test_create_token_unauthorized(api_client):
    response = api_client.post(
        "/api/v0/create_token/",
        json={"email": "foo@bar.com"},
    )
    assert response.status_code == 403


@pytest.mark.with_token(admin=True)
@pytest.mark.parametrize(
    ("hosts", "expected_hosts"),
    [
        (["https://example.com"], ["example.com"]),
        (
            ["sub.example.com"],
            ["sub.example.com"],
        ),
    ],
)
def test_create_token_with_hosts(api_client, hosts, expected_hosts):
    payload = {"email": "widget@example.com"}
    if hosts is not None:
        payload["hosts"] = hosts

    response = api_client.post("/api/v0/create_token/", json=payload)

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data == {"access": ANY, "allowed_hosts": expected_hosts}

    expected_token_payload = {"sub": "widget@example.com", "admin": False}
    if expected_hosts:
        expected_token_payload["allowed_hosts"] = expected_hosts
    assert auth.verify_token(token=resp_data["access"]) == expected_token_payload


def test_robots(api_client):
    response = api_client.get("/robots.txt")
    assert response.content == b"User-agent: *\nAllow: /api/v0/docs\nDisallow: /\n"
