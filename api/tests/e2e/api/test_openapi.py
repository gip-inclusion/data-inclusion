import json

import pytest


@pytest.mark.settings(ENV="prod")
def test_openapi_spec(api_client, snapshot):
    url = "/api/openapi.json"
    response = api_client.get(url)
    spec = response.json()

    assert response.status_code == 200
    assert json.dumps(spec, indent=2, ensure_ascii=False, sort_keys=True) == snapshot


def test_extra_is_undocumented(api_client):
    url = "/api/openapi.json"
    response = api_client.get(url)
    spec = response.json()

    # Check that "extra" is not in the schema for Service
    service_schema = spec["components"]["schemas"]["Service"]
    assert "extra" not in service_schema["properties"]
