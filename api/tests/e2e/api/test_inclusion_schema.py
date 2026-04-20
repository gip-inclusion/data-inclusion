from unittest.mock import ANY

import pytest

from data_inclusion.schema import v1

MODELS_MAPPING = {
    "frais": v1.Frais,
    "reseaux-porteurs": v1.ReseauPorteur,
    "modes-accueil": v1.ModeAccueil,
    "modes-mobilisation": v1.ModeMobilisation,
    "personnes-mobilisatrices": v1.PersonneMobilisatrice,
    "publics": v1.Public,
    "thematiques": v1.Thematique,
    "types-services": v1.TypeService,
}


@pytest.mark.parametrize(
    "collection",
    [
        "frais",
        "reseaux-porteurs",
        "modes-accueil",
        "modes-mobilisation",
        "personnes-mobilisatrices",
        "publics",
        "thematiques",
        "types-services",
    ],
)
@pytest.mark.with_token
def test_list_collections(api_client, collection):
    url = f"/api/v1/doc/{collection}/"

    response = api_client.get(url)

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data[0] == {"value": ANY, "label": ANY, "description": ANY}
    assert set(v.value for v in list(MODELS_MAPPING[collection])) == set(
        d["value"] for d in resp_data
    )
