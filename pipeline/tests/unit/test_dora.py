import pandas as pd
import pytest

from data_inclusion.scripts.tasks.sources import dora


@pytest.fixture
def dora_sample_structure_df():
    return pd.DataFrame(
        [
            {
                "siret": "78341433700247",
                "codeSafirPe": None,
                "typology": None,
                "id": "3105204c-f48c-441a-9cd4-02f9dbd58eb1",
                "name": "ALYS",
                "shortDesc": "",
                "fullDesc": "",
                "url": "",
                "phone": "",
                "email": "",
                "postalCode": "55100",
                "cityCode": "55545",
                "city": "VERDUN",
                "department": "55",
                "address1": "5 RUE DU DOCTEUR ALEXIS CARREL",
                "address2": "SIEGE MEUSE - RDC",
                "ape": "94.99Z",
                "longitude": 5.402447,
                "latitude": 49.150892,
                "creationDate": "2022-03-26",
                "modificationDate": "2022-03-26",
                "source": {"value": "porteur", "label": "Porteur"},
                "linkOnSource": "https://dora.fabrique.social.gouv.fr/structures/alys-kloh",
                "services": [
                    "https://api.dora.fabrique.social.gouv.fr/api/v1/services/511b238c-ff2b-476c-8761-4afdfe9a42a9/",
                ],
            }
        ]
    )


def test_transform_dora_structure(dora_sample_structure_df):
    df = dora.transform_structure_dataframe(dora_sample_structure_df)

    assert df.to_dict(orient="records") == [
        {
            "id": "3105204c-f48c-441a-9cd4-02f9dbd58eb1",
            "siret": "78341433700247",
            "rna": None,
            "nom": "ALYS",
            "commune": "VERDUN",
            "code_postal": "55100",
            "code_insee": "55545",
            "adresse": "5 RUE DU DOCTEUR ALEXIS CARREL",
            "complement_adresse": "SIEGE MEUSE - RDC",
            "longitude": 5.402447,
            "latitude": 49.150892,
            "typologie": None,
            "telephone": None,
            "courriel": None,
            "site_web": None,
            "presentation_resume": None,
            "presentation_detail": None,
            "source": "dora",
            "date_maj": "2022-03-25T23:00:00+00:00",
            "antenne": False,
            "lien_source": "https://dora.fabrique.social.gouv.fr/structures/alys-kloh",
            "horaires_ouverture": None,
            "accessibilite": None,
            "labels_nationaux": None,
            "labels_autres": None,
            "thematiques": None,
        }
    ]


@pytest.fixture
def dora_sample_service_df():
    return pd.DataFrame(
        [
            {
                "id": "511b238c-ff2b-476c-8761-4afdfe9a42a9",
                "name": "TISF",
                "shortDesc": "Accompagnement des familles à domicile",
                "fullDesc": "",
                "kinds": [
                    {"value": "accompagnement", "label": "Accompagnement"},
                    {"value": "accueil", "label": "Accueil"},
                    {"value": "aide-materielle", "label": "Aide materielle"},
                    {"value": "formation", "label": "Formation"},
                    {"value": "information", "label": "Information"},
                ],
                "categories": [
                    {"value": "famille", "label": "Famille"},
                    {"value": "mobilite", "label": "Mobilite"},
                    {"value": "numerique", "label": "Numérique"},
                ],
                "subcategories": [
                    {
                        "value": "famille--accompagnement-parents",
                        "label": "Information et accompagnement des parents",
                    },
                    {
                        "value": "numerique--favoriser-mon-insertion-professionnelle",
                        "label": "Favoriser mon insertion professionnelle",
                    },
                ],
                "accessConditions": ["Sans condition"],
                "concernedPublic": [
                    "Public femme en difficulté",
                    "Parent isolé",
                    "Bénéficiaire de prestation familiale",
                    "Personne en situation de handicap",
                    "Demandeur d'emploi",
                    "Parents",
                    "Famille/enfants",
                ],
                "isCumulative": True,
                "hasFee": False,
                "feeDetails": "",
                "beneficiariesAccessModes": [
                    {"value": "envoyer-courriel", "label": "Envoyer un mail"},
                    {"value": "se-presenter", "label": "Se présenter"},
                    {"value": "telephoner", "label": "Téléphoner"},
                ],
                "beneficiariesAccessModesOther": "",
                "coachOrientationModes": [
                    {"value": "envoyer-courriel", "label": "Envoyer un mail"},
                    {"value": "telephoner", "label": "Téléphoner"},
                ],
                "coachOrientationModesOther": "",
                "requirements": ["Aucun"],
                "credentials": [],
                "onlineForm": "https://www.alys.fr/familles/etre-accompagne-a-domicile",
                "locationKinds": [{"value": "en-presentiel", "label": "En présentiel"}],
                "remoteUrl": "",
                "address1": "5 RUE DU DOCTEUR ALEXIS CARREL",
                "address2": "SIEGE MEUSE - RDC",
                "postalCode": "55100",
                "cityCode": "55545",
                "city": "VERDUN",
                "longitude": 5.402447,
                "latitude": 49.150892,
                "diffusionZoneType": "Département",
                "diffusionZoneDetails": "55",
                "qpvOrZrr": False,
                "recurrence": "",
                "suspensionDate": None,
                "structure": "https://api.dora.fabrique.social.gouv.fr/api/v1/structures/3105204c-f48c-441a-9cd4-02f9dbd58eb1/",
                "creationDate": "2022-03-29",
                "modificationDate": "2022-07-23",
                "publicationDate": None,
                "linkOnSource": "https://dora.fabrique.social.gouv.fr/services/alys-kloh-tisf-qfpi",
            }
        ]
    )


def test_transform_dora_service(dora_sample_service_df):
    df = dora.transform_service_dataframe(dora_sample_service_df)

    assert df.to_dict(orient="records") == [
        {
            "id": "511b238c-ff2b-476c-8761-4afdfe9a42a9",
            "structure_id": "3105204c-f48c-441a-9cd4-02f9dbd58eb1",
            "source": "dora",
            "nom": "TISF",
            "presentation_resume": "Accompagnement des familles à domicile",
            "types": [
                "accompagnement",
                "accueil",
                "aide-materielle",
                "formation",
                "information",
            ],
            "thematiques": [
                "famille",
                "mobilite",
                "numerique",
                "numerique--favoriser-mon-insertion-professionnelle",
            ],
            "prise_rdv": "https://www.alys.fr/familles/etre-accompagne-a-domicile",
            "frais": None,
            "frais_autres": None,
            "profils": None,
        }
    ]
