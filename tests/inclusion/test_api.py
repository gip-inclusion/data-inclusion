from unittest.mock import ANY

import pytest

from django.urls import reverse

pytestmark = pytest.mark.django_db


def test_create_report(api_client, structure, user):
    url = reverse("v0:reports-list")
    data = {
        "typology": None,
        "branch_id": "",
        "slug": "droite-sentier",
        "name": "Hebert",
        "short_desc": "Cependant sec.",
        "url": "https://www.gonzalez.net/",
        "full_desc": "Entrée camarade noir espoir.",
        "phone": "0102030405",
        "email": "aurelie01@example.org",
        "postal_code": "09891",
        "city_code": "13991",
        "city": "Robinboeuf",
        "department": "15",
        "address1": "rue de Leclercq",
        "address2": "",
        "code_ape": "84.11Z",
        "longitude": -80.693947,
        "latitude": -56.7421445,
        "geocoding_score": 0.5,
        "source": "",
        "structure": str(structure.id),
        "reporter": user.id,
        "parent": None,
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
                    "typology": None,
                    "branch_id": "",
                    "slug": "droite-sentier",
                    "name": "Hebert",
                    "short_desc": "Cependant sec.",
                    "url": "https://www.gonzalez.net/",
                    "full_desc": "Entrée camarade noir espoir.",
                    "phone": "0102030405",
                    "email": "aurelie01@example.org",
                    "postal_code": "09891",
                    "city_code": "13991",
                    "city": "Robinboeuf",
                    "department": "15",
                    "address1": "rue de Leclercq",
                    "address2": "",
                    "code_ape": "84.11Z",
                    "longitude": -80.693947,
                    "latitude": -56.7421445,
                    "geocoding_score": 0.5,
                    "source": "",
                    "creation_date": None,
                    "modification_date": None,
                    "extra": {},
                    "parent": None,
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
                    "typology": None,
                    "branch_id": "",
                    "slug": "facon-bataille",
                    "name": "Morin et Fils",
                    "short_desc": "Mince vous trait.",
                    "url": "http://berger.fr/",
                    "full_desc": "Continuer monde demi verser.",
                    "phone": "0102030405",
                    "email": "levyxavier@example.net",
                    "postal_code": "98947",
                    "city_code": "96593",
                    "city": "Gonzalez",
                    "department": "24",
                    "address1": "5, rue Emmanuel Potier",
                    "address2": "",
                    "code_ape": "84.11Z",
                    "longitude": -110.086283,
                    "latitude": -49.8553885,
                    "geocoding_score": 0.5,
                    "source": "",
                    "creation_date": ANY,
                    "modification_date": ANY,
                    "extra": {},
                    "parent": None,
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
