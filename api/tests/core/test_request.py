import pytest

from data_inclusion.api.core.request import models


@pytest.mark.with_token
def test_save_api_request_with_token(api_client, test_session):
    test_session.query(models.Request).delete()
    # FIXME(vperron) : The issue here is that the middleware writes the
    # APIRequest object, but it is initialized through a direct reference
    # to the "normal" database Engine. This means that the API Request is
    # created with the "normal" database.
    # I could not find a proper dependency injection system that would
    # initialize the middleware so that it accesses the "test" engine or
    # session. Have to look harder.
    # Also I need to test the 307 status code case.
    url = "/api/v0/structures/foo/bar?baz=1"

    response = api_client.get(url)

    assert response.status_code == 404
    # Ici on recupere des resulttats de la DB de test mais les api_request
    # sont sauvés dans la DB de production.
    assert test_session.query(models.Request).count() == 1

    request_instance = test_session.query(models.Request).first()
    assert request_instance.status_code == 404
    assert request_instance.user == "some_user"
    assert request_instance.path == "/api/v0/structures/foo/bar"
    assert request_instance.method == "GET"
    assert request_instance.path_params == {"source": "foo", "id": "bar"}
    assert request_instance.query_params == {"baz": "1"}
    assert request_instance.endpoint_name == "retrieve_structure_endpoint"


def test_save_api_request_without_token(api_client, test_session):
    test_session.query(models.Request).delete()
    url = "/api/v0/structures"

    response = api_client.get(url)

    assert response.status_code == 403
    assert test_session.query(models.Request).count() == 1

    request_instance = test_session.query(models.Request).first()
    assert request_instance.status_code == 403
    assert request_instance.user is None
    assert request_instance.path == "/api/v0/structures"
    assert request_instance.method == "GET"
    assert request_instance.path_params == {}
    assert request_instance.query_params == {}
    assert request_instance.endpoint_name == "list_structures_endpoint"
