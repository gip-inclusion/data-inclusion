from unittest.mock import ANY

import pytest

from django.urls import reverse

pytestmark = pytest.mark.django_db


def test_create_report(api_client, structure, user):
    url = reverse("v0:reports-list")
    data = {
        "typologie": None,
        "id_antenne": "",
        "nom": "Hebert",
        "presentation_resume": "Cependant sec.",
        "site_web": "https://www.gonzalez.net/",
        "presentation_detail": "Entrée camarade noir espoir.",
        "telephone": "0102030405",
        "courriel": "aurelie01@example.org",
        "code_postal": "09891",
        "code_insee": "13991",
        "commune": "Robinboeuf",
        "departement": "15",
        "adresse": "rue de Leclercq",
        "complement_adresse": "",
        "longitude": -80.693947,
        "latitude": -56.7421445,
        "score_geocodage": 0.5,
        "source": "",
        "structure": str(structure.id),
        "reporter": user.id,
        "structure_mere": None,
    }
    response = api_client.post(url, data, format="json")

    assert response.status_code == 201

    url = reverse("v0:structures-detail", kwargs={"pk": structure.id})
    response = api_client.get(url)

    assert response.json() == {
        "id": ANY,
        "latest_reports": [
            {
                "id": ANY,
                "data": {
                    "id": ANY,
                    "typologie": None,
                    "id_antenne": "",
                    "nom": "Hebert",
                    "presentation_resume": "Cependant sec.",
                    "site_web": "https://www.gonzalez.net/",
                    "presentation_detail": "Entrée camarade noir espoir.",
                    "telephone": "0102030405",
                    "courriel": "aurelie01@example.org",
                    "code_postal": "09891",
                    "code_insee": "13991",
                    "commune": "Robinboeuf",
                    "departement": "15",
                    "adresse": "rue de Leclercq",
                    "complement_adresse": "",
                    "longitude": -80.693947,
                    "latitude": -56.7421445,
                    "score_geocodage": 0.5,
                    "source": "",
                    "date_maj": None,
                    "extra": {},
                    "structure_mere": None,
                },
                "reporter": "",
                "created_at": ANY,
                "updated_at": ANY,
            }
        ],
        "updated_at": ANY,
        "created_at": ANY,
        "siret": "60487647500499",
        "rna": "W382421948",
        "code_safir_pe": None,
        "name": "Ruiz Perrin et Fils",
        "ape": "84.11Z",
        "address1": "chemin de Georges",
        "address2": "",
        "city": "Rousselboeuf",
        "city_code": "59387",
        "postal_code": "84080",
        "is_siege": True,
        "longitude": -129.925049,
        "latitude": 17.058379,
    }


def test_retrieve_structure(api_client, structure, structure_report):
    url = reverse("v0:structures-detail", kwargs={"pk": structure.id})
    response = api_client.get(url)
    rdata = response.json()

    assert rdata == {
        "id": ANY,
        "latest_reports": [
            {
                "id": ANY,
                "data": {
                    "id": ANY,
                    "typologie": None,
                    "id_antenne": "",
                    "nom": "Raymond S.A.R.L.",
                    "presentation_resume": "Instant état image.",
                    "site_web": "http://www.dupuis.fr/",
                    "presentation_detail": "Avis ensemble livrer toute.",
                    "telephone": "0102030405",
                    "courriel": "susannemarion@example.net",
                    "code_postal": "71965",
                    "code_insee": "42320",
                    "commune": "Descamps-sur-Bertin",
                    "departement": "60",
                    "adresse": "rue Gérard",
                    "complement_adresse": "",
                    "longitude": -159.254468,
                    "latitude": -68.4585065,
                    "score_geocodage": 0.5,
                    "source": "",
                    "date_maj": ANY,
                    "extra": {},
                    "structure_mere": None,
                },
                "reporter": "",
                "created_at": ANY,
                "updated_at": ANY,
            }
        ],
        "updated_at": ANY,
        "created_at": ANY,
        "siret": "60487647500499",
        "rna": "W382421948",
        "code_safir_pe": None,
        "name": "Ruiz Perrin et Fils",
        "ape": "84.11Z",
        "address1": "chemin de Georges",
        "address2": "",
        "city": "Rousselboeuf",
        "city_code": "59387",
        "postal_code": "84080",
        "is_siege": True,
        "longitude": -129.925049,
        "latitude": 17.058379,
    }

    # l'identifiant de la structure est utilisé dans ses représentations
    assert rdata["latest_reports"][0]["data"]["id"] == rdata["id"]
