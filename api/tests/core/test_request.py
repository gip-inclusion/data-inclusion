import pytest

from data_inclusion.api.core.request import models


@pytest.mark.with_token
def test_save_api_request_with_token(api_client, db_session):
    url = "/api/v0/structures/foo/bar?baz=1"

    response = api_client.get(url)

    assert response.status_code == 404
    assert db_session.query(models.Request).count() == 1

    request_instance = db_session.query(models.Request).first()
    assert request_instance.status_code == 404
    assert request_instance.user == "some_user"
    assert request_instance.path == "/api/v0/structures/foo/bar"
    assert request_instance.method == "GET"
    assert request_instance.path_params == {"source": "foo", "id": "bar"}
    assert request_instance.query_params == {"baz": "1"}
    assert request_instance.endpoint_name == "retrieve_structure_endpoint"


def test_save_api_request_without_token(api_client, db_session):
    url = "/api/v0/structures"

    response = api_client.get(url)

    assert response.status_code == 403
    assert db_session.query(models.Request).count() == 0
