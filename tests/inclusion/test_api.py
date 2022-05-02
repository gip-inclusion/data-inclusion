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
        "siteweb": "https://www.gonzalez.net/",
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
                    "site_web": "",
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
        "code_safir_pe": None,
        "name": "Bodin Joly SARL",
        "ape": "84.11Z",
        "address1": "5, boulevard de Caron",
        "address2": "",
        "city": "Perrin",
        "city_code": "57815",
        "postal_code": "59387",
        "is_siege": True,
        "longitude": 57.669186,
        "latitude": 49.9498015,
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
                    "nom": "Bonnet",
                    "presentation_resume": "Regarder agiter.",
                    "site_web": "https://www.le.net/",
                    "presentation_detail": "Or personne jambe.",
                    "telephone": "0102030405",
                    "courriel": "bonninveronique@example.net",
                    "code_postal": "84833",
                    "code_insee": "94775",
                    "commune": "Sainte JulietteBourg",
                    "departement": "62",
                    "adresse": "43, chemin Ferreira",
                    "complement_adresse": "",
                    "longitude": -61.64115,
                    "latitude": 9.8741475,
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
        "code_safir_pe": None,
        "name": "Bodin Joly SARL",
        "ape": "84.11Z",
        "address1": "5, boulevard de Caron",
        "address2": "",
        "city": "Perrin",
        "city_code": "57815",
        "postal_code": "59387",
        "is_siege": True,
        "longitude": 57.669186,
        "latitude": 49.9498015,
    }

    # l'identifiant de la structure est utilisé dans ses représentations
    assert rdata["latest_reports"][0]["data"]["id"] == rdata["id"]
