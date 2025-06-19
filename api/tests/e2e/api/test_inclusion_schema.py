from unittest.mock import ANY

import pytest

from data_inclusion.schema import v0, v1


@pytest.fixture
def model(schema_version, framework):
    schema = v1 if schema_version == "v1" else v0
    return getattr(
        schema,
        {
            "labels-nationaux": "LabelNational",
            "thematiques": "Thematique",
            "typologies-services": "TypologieService",
            "frais": "Frais",
            "profils": "Profil",
            "typologies-structures": "TypologieStructure",
            "modes-accueil": "ModeAccueil",
            "modes-orientation-accompagnateur": "ModeOrientationAccompagnateur",
            "modes-orientation-beneficiaire": "ModeOrientationBeneficiaire",
            "modes-mobilisation": "ModeMobilisation",
            "personnes-mobilisatrice": "PersonneMobilisatrice",
        }[framework],
    )


@pytest.mark.parametrize(
    ("framework", "schema_version"),
    [
        *[
            (framework, "v0")
            for framework in [
                "labels-nationaux",
                "thematiques",
                "typologies-services",
                "frais",
                "profils",
                "typologies-structures",
                "modes-accueil",
                "modes-orientation-accompagnateur",
                "modes-orientation-beneficiaire",
            ]
        ],
        *[
            (framework, "v1")
            for framework in [
                "labels-nationaux",
                "thematiques",
                "typologies-services",
                "frais",
                "profils",
                "typologies-structures",
                "modes-accueil",
                "modes-mobilisation",
                "personnes-mobilisatrice",
            ]
        ],
    ],
)
@pytest.mark.with_token
def test_list_framework(api_client, schema_version, framework, model):
    url = f"/api/{schema_version}/doc/{framework}/"

    response = api_client.get(url)

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data[0] == {"value": ANY, "label": ANY, "description": ANY}
    assert set(v.value for v in list(model)) == set(d["value"] for d in resp_data)
