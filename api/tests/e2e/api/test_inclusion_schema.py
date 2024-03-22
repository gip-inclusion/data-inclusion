from unittest.mock import ANY

import pytest


@pytest.mark.parametrize(
    "framework",
    [
        "labels-nationaux",
        "thematiques",
        "typologies-services",
        "frais",
        "profils",
        "typologies-structures",
        "modes-accueil",
        "modes-orientation-accompagnateur",
        "modes-orientation-beneficiaire",
    ],
)
@pytest.mark.with_token
def test_list_framework(api_client, framework):
    url = f"/api/v0/doc/{framework}/"

    response = api_client.get(url)

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data[0] == {"value": ANY, "label": ANY, "description": ANY}
