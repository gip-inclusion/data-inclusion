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
    "included_in_schema",
    [
        pytest.param(False, id="prod", marks=[pytest.mark.env("prod")]),
        pytest.param(True, id="staging", marks=[pytest.mark.env("staging")]),
        pytest.param(True, id="test", marks=[pytest.mark.env("test")]),
        pytest.param(True, id="dev", marks=[pytest.mark.env("dev")]),
    ],
)
def test_v1_include_in_schema(api_client, included_in_schema):
    url = "/api/openapi.json"
    response = api_client.get(url)

    assert response.status_code == 200
    assert included_in_schema == ("v1" in response.text)
