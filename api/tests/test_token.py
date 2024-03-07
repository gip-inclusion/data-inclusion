from unittest.mock import ANY

import pytest

from data_inclusion.api.auth import services


def test_create_token_unauthenticated(api_client):
    url = "/api/v0/create_token/"
    response = api_client.post(url, json={"email": "foo@bar.com"})

    assert response.status_code == 403


@pytest.mark.with_token
def test_create_token_unauthorized(api_client):
    url = "/api/v0/create_token/"
    response = api_client.post(url, json={"email": "foo@bar.com"})

    assert response.status_code == 401


@pytest.mark.with_admin_token
def test_create_token(api_client):
    url = "/api/v0/create_token/"
    response = api_client.post(url, json={"email": "foo@bar.com"})

    assert response.status_code == 200

    resp_data = response.json()

    assert resp_data == {"access": ANY}
    assert services.verify_token(token=resp_data["access"]) == {
        "sub": "foo@bar.com",
        "admin": False,
    }
