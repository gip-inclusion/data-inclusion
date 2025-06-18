from unittest.mock import ANY

import pytest

from data_inclusion.schema import v0, v1

MODELS_MAPPING_V0 = {
    "frais": v0.Frais,
    "labels-nationaux": v0.LabelNational,
    "modes-accueil": v0.ModeAccueil,
    "modes-orientation-accompagnateur": v0.ModeOrientationAccompagnateur,
    "modes-orientation-beneficiaire": v0.ModeOrientationBeneficiaire,
    "profils": v0.Profil,
    "thematiques": v0.Thematique,
    "typologies-services": v0.TypologieService,
    "typologies-structures": v0.TypologieStructure,
}


MODELS_MAPPING_V1 = {
    "frais": v1.Frais,
    "labels-nationaux": v1.LabelNational,
    "modes-accueil": v1.ModeAccueil,
    "modes-mobilisation": v1.ModeMobilisation,
    "personnes-mobilisatrice": v1.PersonneMobilisatrice,
    "publics": v1.Public,
    "thematiques": v1.Thematique,
    "typologies-services": v1.TypologieService,
    "typologies-structures": v1.TypologieStructure,
}


@pytest.mark.parametrize(
    "collection",
    [
        "frais",
        "labels-nationaux",
        "modes-accueil",
        "modes-orientation-accompagnateur",
        "modes-orientation-beneficiaire",
        "profils",
        "thematiques",
        "typologies-services",
        "typologies-structures",
    ],
)
@pytest.mark.with_token
def test_list_collections_v0(api_client, collection):
    url = f"/api/v0/doc/{collection}/"

    response = api_client.get(url)

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data[0] == {"value": ANY, "label": ANY, "description": ANY}
    assert set(v.value for v in list(MODELS_MAPPING_V0[collection])) == set(
        d["value"] for d in resp_data
    )


@pytest.mark.parametrize(
    "collection",
    [
        "frais",
        "labels-nationaux",
        "modes-accueil",
        "modes-mobilisation",
        "personnes-mobilisatrice",
        "publics",
        "thematiques",
        "typologies-services",
        "typologies-structures",
    ],
)
@pytest.mark.with_token
def test_list_collections_v1(api_client, collection):
    url = f"/api/v1/doc/{collection}/"

    response = api_client.get(url)

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data[0] == {"value": ANY, "label": ANY, "description": ANY}
    assert set(v.value for v in list(MODELS_MAPPING_V1[collection])) == set(
        d["value"] for d in resp_data
    )
