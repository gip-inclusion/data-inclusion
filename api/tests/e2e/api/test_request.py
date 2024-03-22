import pytest
import sqlalchemy as sqla

from data_inclusion.api.request import models


@pytest.mark.with_token
def test_save_api_request_with_token(api_client, test_session):
    url = "/api/v0/structures/foo/bar?baz=1"
    response = api_client.get(url)

    assert response.status_code == 404
    assert (
        test_session.scalar(sqla.select(sqla.func.count()).select_from(models.Request))
        == 1
    )

    request_instance = test_session.scalars(sqla.select(models.Request)).first()
    assert request_instance.status_code == 404
    assert request_instance.user == "some_user"
    assert request_instance.path == "/api/v0/structures/foo/bar"
    assert request_instance.method == "GET"
    assert request_instance.path_params == {"source": "foo", "id": "bar"}
    assert request_instance.query_params == {"baz": "1"}
    assert request_instance.endpoint_name == "retrieve_structure_endpoint"


@pytest.mark.with_token
def test_ignore_redirect(api_client, test_session):
    url = "/api/v0/structures/"
    response = api_client.get(url)

    assert response.status_code == 200
    assert (
        test_session.scalar(sqla.select(sqla.func.count()).select_from(models.Request))
        == 1
    )


def test_save_api_request_without_token(api_client, test_session):
    url = "/api/v0/structures"
    response = api_client.get(url)

    assert response.status_code == 403
    assert (
        test_session.scalar(sqla.select(sqla.func.count()).select_from(models.Request))
        == 0
    )
