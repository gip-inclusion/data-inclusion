from unittest.mock import ANY

import pytest

from data_inclusion.api import schema


def test_list_structures_unauthenticated(api_client):
    url = "/api/v0/structures/"
    response = api_client.get(url)

    assert response.status_code == 403


@pytest.mark.with_token
def test_list_structures_all(api_client, structure_factory):
    structure_factory()

    url = "/api/v0/structures/"

    response = api_client.get(url)
    resp_data = response.json()

    assert resp_data == {
        "items": [
            {
                "id": "matiere-nom-asseoir",
                "siret": "76475938200654",
                "rna": "W219489241",
                "nom": "Pottier SARL",
                "commune": "VaillantBourg",
                "code_postal": "65938",
                "code_insee": "78408",
                "adresse": "avenue Lacombe",
                "complement_adresse": None,
                "longitude": 178.712016,
                "latitude": 77.843518,
                "typologie": "ACI",
                "telephone": "0102030405",
                "courriel": "raymondclemence@example.com",
                "site_web": "http://aubert.net/",
                "presentation_resume": "Espèce couler.",
                "presentation_detail": "Or personne jambe.",
                "source": "dora",
                "date_maj": ANY,
                "structure_parente": None,
                "lien_source": "https://dora.fr/matiere-nom-asseoir",
                "horaires_ouverture": 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                "accessibilite": "https://acceslibre.beta.gouv.fr/app/matiere-nom-asseoir/",
                "labels_nationaux": [],
                "labels_autres": ["SudLabs", "Nièvre médiation numérique"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
    }


@pytest.mark.with_token
def test_list_structures_filter_by_typology(api_client, structure_factory):
    structure_factory(typologie=schema.Typologie.ASSO.value)
    structure_factory(typologie=schema.Typologie.CCAS.value)

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"typologie": schema.Typologie.ASSO.value})

    assert response.json() == {
        "items": [
            {
                "id": "matiere-nom-asseoir",
                "siret": "76475938200654",
                "rna": "W219489241",
                "nom": "Pottier SARL",
                "commune": "VaillantBourg",
                "code_postal": "65938",
                "code_insee": "78408",
                "adresse": "avenue Lacombe",
                "complement_adresse": None,
                "longitude": 178.712016,
                "latitude": 77.843518,
                "typologie": "ASSO",
                "telephone": "0102030405",
                "courriel": "raymondclemence@example.com",
                "site_web": "http://aubert.net/",
                "presentation_resume": "Espèce couler.",
                "presentation_detail": "Or personne jambe.",
                "source": "itou",
                "date_maj": ANY,
                "structure_parente": None,
                "lien_source": "https://itou.fr/matiere-nom-asseoir",
                "horaires_ouverture": 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                "accessibilite": "https://acceslibre.beta.gouv.fr/app/matiere-nom-asseoir/",
                "labels_nationaux": [],
                "labels_autres": ["SudLabs", "Nièvre médiation numérique"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": 50,
    }

    response = api_client.get(url, params={"typologie": schema.Typologie.MUNI.value})
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": 50}


@pytest.mark.with_token
def test_list_structures_filter_by_label(api_client, structure_factory):
    structure_factory(labels_nationaux=[schema.LabelNational.POLE_EMPLOI.value])
    structure_factory(
        labels_nationaux=[
            schema.LabelNational.MOBIN.value,
            schema.LabelNational.FRANCE_SERVICE.value,
        ]
    )

    url = "/api/v0/structures/"
    response = api_client.get(
        url, params={"label_national": schema.LabelNational.FRANCE_SERVICE.value}
    )

    assert response.json() == {
        "items": [
            {
                "id": "cacher-violent",
                "siret": "68483396900874",
                "rna": "W775159179",
                "nom": "Aubert",
                "commune": "Durand",
                "code_postal": "13525",
                "code_insee": "01230",
                "adresse": "191, rue Seguin",
                "complement_adresse": None,
                "longitude": 129.212387,
                "latitude": -57.869491,
                "typologie": "AFPA",
                "telephone": "0102030405",
                "courriel": "xrobin@example.org",
                "site_web": "http://www.gonzalez.fr/",
                "presentation_resume": "Écraser bas un an.",
                "presentation_detail": "Lieu apparence bon voir.",
                "source": "dora",
                "date_maj": ANY,
                "structure_parente": None,
                "lien_source": "https://dora.fr/cacher-violent",
                "horaires_ouverture": 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                "accessibilite": "https://acceslibre.beta.gouv.fr/app/cacher-violent/",
                "labels_nationaux": ["MOBIN", "FRANCE_SERVICE"],
                "labels_autres": ["SudLabs", "Nièvre médiation numérique"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": 50,
    }

    response = api_client.get(
        url, params={"label_national": schema.LabelNational.AFPA.value}
    )
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": 50}
