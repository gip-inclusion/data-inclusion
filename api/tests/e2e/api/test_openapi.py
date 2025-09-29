import json

import pytest


def test_openapi_spec(api_client, snapshot):
    url = "/api/openapi.json"
    response = api_client.get(url)

    assert response.status_code == 200
    assert (
        json.dumps(response.json(), indent=2, ensure_ascii=False, sort_keys=True)
        == snapshot
    )


@pytest.mark.parametrize(
    (),
    [
        pytest.param(id="prod", marks=[pytest.mark.settings(ENV="prod")]),
        pytest.param(id="staging", marks=[pytest.mark.settings(ENV="staging")]),
        pytest.param(id="test", marks=[pytest.mark.settings(ENV="test")]),
        pytest.param(id="dev", marks=[pytest.mark.settings(ENV="dev")]),
    ],
)
def test_v1_include_in_schema(api_client):
    url = "/api/openapi.json"
    response = api_client.get(url)

    assert response.status_code == 200
