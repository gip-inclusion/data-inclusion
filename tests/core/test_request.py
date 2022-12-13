import pytest

from data_inclusion.api.core.request import models


@pytest.mark.with_token
def test_save_api_request_with_token(api_client, db_session):
    url = "/api/v0/structures"

    response = api_client.get(url)

    assert response.status_code == 200
    assert db_session.query(models.Request).count() == 1

    request_instance = db_session.query(models.Request).first()
    assert request_instance.status_code == 200
    assert request_instance.user == "some_user"
    assert request_instance.path == "/api/v0/structures"
    assert request_instance.method == "GET"


def test_save_api_request_without_token(api_client, db_session):
    url = "/api/v0/structures"

    response = api_client.get(url)

    assert response.status_code == 403
    assert db_session.query(models.Request).count() == 1

    request_instance = db_session.query(models.Request).first()
    assert request_instance.status_code == 403
    assert request_instance.user is None
    assert request_instance.path == "/api/v0/structures"
    assert request_instance.method == "GET"
