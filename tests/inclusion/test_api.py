from unittest.mock import ANY

import pytest

from django.urls import reverse

pytestmark = pytest.mark.django_db


def test_structure(api_client, structure, structure_report):
    url = reverse("v0:structures-detail", kwargs={"pk": structure.id})
    response = api_client.get(url)

    assert response.json() == {
        "id": ANY,
        "latest_reports": [
            {
                "id": ANY,
                "typology": None,
                "updated_at": ANY,
                "created_at": ANY,
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
                "structure": ANY,
                "reporter": 1,
                "parent": None,
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
