from unittest.mock import ANY

import pytest

from django.urls import reverse

pytestmark = pytest.mark.django_db


def test_create_report(api_client, structure, user):
    url = reverse("v0:reports-list")
    data = {
        "typologie": None,
        "id_antenne": "",
        "slug": "droite-sentier",
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
        "code_ape": "84.11Z",
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
                    "slug": "droite-sentier",
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
                    "code_ape": "84.11Z",
                    "longitude": -80.693947,
                    "latitude": -56.7421445,
                    "score_geocodage": 0.5,
                    "source": "",
                    "date_creation": None,
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
                    "slug": "facon-bataille",
                    "nom": "Morin et Fils",
                    "presentation_resume": "Mince vous trait.",
                    "site_web": "http://berger.fr/",
                    "presentation_detail": "Continuer monde demi verser.",
                    "telephone": "0102030405",
                    "courriel": "levyxavier@example.net",
                    "code_postal": "98947",
                    "code_insee": "96593",
                    "commune": "Gonzalez",
                    "departement": "24",
                    "adresse": "5, rue Emmanuel Potier",
                    "complement_adresse": "",
                    "code_ape": "84.11Z",
                    "longitude": -110.086283,
                    "latitude": -49.8553885,
                    "score_geocodage": 0.5,
                    "source": "",
                    "date_creation": ANY,
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
