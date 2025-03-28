from unittest.mock import ANY

import pytest

from data_inclusion.api import auth


def test_create_token_unauthenticated(api_client):
    url = "/api/v0/create_token/"
    response = api_client.post(url, json={"email": "foo@bar.com"})

    assert response.status_code == 403


@pytest.mark.with_token
def test_create_token_unauthorized(api_client):
    url = "/api/v0/create_token/"
    response = api_client.post(url, json={"email": "foo@bar.com"})

    assert response.status_code == 403


@pytest.mark.with_token(admin=True)
def test_create_token(api_client):
    url = "/api/v0/create_token/"
    response = api_client.post(url, json={"email": "foo@bar.com"})

    assert response.status_code == 200

    resp_data = response.json()

    assert resp_data == {"access": ANY}
    assert auth.verify_token(token=resp_data["access"]) == {
        "sub": "foo@bar.com",
        "admin": False,
    }


def test_robots(api_client):
    response = api_client.get("/robots.txt")
    assert response.content == b"User-agent: *\nAllow: /api/v0/docs\nDisallow: /\n"
