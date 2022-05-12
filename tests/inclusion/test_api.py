from unittest.mock import ANY

import pytest

from django.urls import reverse

pytestmark = pytest.mark.django_db


def test_create_report(api_client, structure):
    url = reverse("v0:reports-list")
    data = {
        "typologie": None,
        "siret": structure.siret,
        "rna": structure.rna,
        "nom": "Hebert",
        "presentation_resume": "Cependant sec.",
        "site_web": "https://www.gonzalez.net/",
        "presentation_detail": "Entrée camarade noir espoir.",
        "telephone": "0102030405",
        "courriel": "aurelie01@example.org",
        "code_postal": "09891",
        "code_insee": "13991",
        "commune": "Robinboeuf",
        "adresse": "rue de Leclercq",
        "complement_adresse": "",
        "longitude": -80.693947,
        "latitude": -56.7421445,
        "score_geocodage": 0.5,
        "source": "dora",
        "date_maj": "2022-04-28T16:53:11Z",
        "structure_parente": None,
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
                    "structure_parente": None,
                    "nom": "Hebert",
                    "siret": "60487647500499",
                    "rna": "W382421948",
                    "presentation_resume": "Cependant sec.",
                    "site_web": "https://www.gonzalez.net/",
                    "presentation_detail": "Entrée camarade noir espoir.",
                    "telephone": "0102030405",
                    "courriel": "aurelie01@example.org",
                    "code_postal": "09891",
                    "code_insee": "13991",
                    "commune": "Robinboeuf",
                    "adresse": "rue de Leclercq",
                    "complement_adresse": "",
                    "longitude": -80.693947,
                    "latitude": -56.7421445,
                    "score_geocodage": 0.5,
                    "source": "dora",
                    "date_maj": "2022-04-28T18:53:11+02:00",
                    "extra": {},
                },
                "antennes_data": [],
                "created_at": ANY,
                "updated_at": ANY,
            }
        ],
        "updated_at": ANY,
        "created_at": ANY,
        "siret": "60487647500499",
        "rna": "W382421948",
    }


def test_create_report_minimal(api_client, structure):
    url = reverse("v0:reports-list")
    data = {
        "siret": structure.siret,
        "nom": "Hebert",
        "code_postal": "09891",
        "code_insee": "13991",
        "commune": "Robinboeuf",
        "adresse": "rue de Leclercq",
        "date_maj": "2022-04-28T16:53:11Z",
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
                    "structure_parente": None,
                    "nom": "Hebert",
                    "siret": "60487647500499",
                    "rna": None,
                    "presentation_resume": "",
                    "site_web": "",
                    "presentation_detail": "",
                    "telephone": "",
                    "courriel": "",
                    "code_postal": "09891",
                    "code_insee": "13991",
                    "commune": "Robinboeuf",
                    "adresse": "rue de Leclercq",
                    "complement_adresse": "",
                    "longitude": None,
                    "latitude": None,
                    "source": "",
                    "date_maj": "2022-04-28T18:53:11+02:00",
                    "score_geocodage": None,
                    "extra": {},
                },
                "created_at": ANY,
                "updated_at": ANY,
                "antennes_data": [],
            }
        ],
        "updated_at": ANY,
        "created_at": ANY,
        "siret": "60487647500499",
        "rna": "W382421948",
    }


def test_create_report_and_structure(api_client):
    url = reverse("v0:reports-list")
    data = {
        "typologie": None,
        "siret": "12345678901234",
        "rna": "W123456789",
        "nom": "Hebert",
        "presentation_resume": "Cependant sec.",
        "site_web": "https://www.gonzalez.net/",
        "presentation_detail": "Entrée camarade noir espoir.",
        "telephone": "0102030405",
        "courriel": "aurelie01@example.org",
        "code_postal": "09891",
        "code_insee": "13991",
        "commune": "Robinboeuf",
        "adresse": "rue de Leclercq",
        "complement_adresse": "",
        "longitude": -80.693947,
        "latitude": -56.7421445,
        "score_geocodage": 0.5,
        "source": "dora",
        "structure_mere": None,
    }
    response = api_client.post(url, data, format="json")

    assert response.status_code == 201
    resp_data = response.json()
    assert resp_data.get("id", None) is not None

    url = reverse("v0:structures-detail", kwargs={"pk": resp_data["id"]})
    response = api_client.get(url)
    resp_data = response.json()

    assert resp_data == {
        "id": ANY,
        "latest_reports": [
            {
                "id": ANY,
                "data": {
                    "id": ANY,
                    "siret": "12345678901234",
                    "rna": "W123456789",
                    "typologie": None,
                    "structure_parente": None,
                    "nom": "Hebert",
                    "presentation_resume": "Cependant sec.",
                    "site_web": "https://www.gonzalez.net/",
                    "presentation_detail": "Entrée camarade noir espoir.",
                    "telephone": "0102030405",
                    "courriel": "aurelie01@example.org",
                    "code_postal": "09891",
                    "code_insee": "13991",
                    "commune": "Robinboeuf",
                    "adresse": "rue de Leclercq",
                    "complement_adresse": "",
                    "longitude": -80.693947,
                    "latitude": -56.7421445,
                    "score_geocodage": 0.5,
                    "source": "dora",
                    "date_maj": None,
                    "extra": {},
                },
                "antennes_data": [],
                "created_at": ANY,
                "updated_at": ANY,
            }
        ],
        "updated_at": ANY,
        "created_at": ANY,
        "siret": "12345678901234",
        "rna": "W123456789",
    }

    # l'identifiant de la structure est utilisé dans ses représentations
    assert resp_data["latest_reports"][0]["data"]["id"] == resp_data["id"]


def test_retrieve_structure(api_client, structure, structure_report):
    url = reverse("v0:structures-detail", kwargs={"pk": structure.id})
    response = api_client.get(url)
    resp_data = response.json()

    assert resp_data == {
        "id": ANY,
        "latest_reports": [
            {
                "id": ANY,
                "data": {
                    "id": ANY,
                    "typologie": None,
                    "structure_parente": None,
                    "nom": "Perrin",
                    "siret": "60487647500499",
                    "rna": "W382421948",
                    "presentation_resume": "Bouche valeur.",
                    "site_web": "https://www.gay.fr/",
                    "presentation_detail": "Or sans espace.",
                    "telephone": "0102030405",
                    "courriel": "frobin@example.com",
                    "code_postal": "14841",
                    "code_insee": "83989",
                    "commune": "Hebert",
                    "adresse": "avenue de Prévost",
                    "complement_adresse": "",
                    "longitude": 129.050286,
                    "latitude": -25.0098555,
                    "source": "dora",
                    "date_maj": ANY,
                    "score_geocodage": 0.5,
                    "extra": {},
                },
                "created_at": ANY,
                "updated_at": ANY,
                "antennes_data": [],
            }
        ],
        "updated_at": ANY,
        "created_at": ANY,
        "siret": "60487647500499",
        "rna": "W382421948",
    }

    # l'identifiant de la structure est utilisé dans ses représentations
    assert resp_data["latest_reports"][0]["data"]["id"] == resp_data["id"]


def test_create_antenne_report(api_client, structure, structure_report):
    url = reverse("v0:reports-list")
    data = {
        "nom": "Hebert",
        "code_postal": "09891",
        "code_insee": "13991",
        "commune": "Robinboeuf",
        "adresse": "rue de Leclercq",
        "date_maj": "2022-04-28T16:53:11Z",
        "structure_parente": structure_report.id_in_source,
        "source": structure_report.source,
    }
    response = api_client.post(url, data, format="json")

    assert response.status_code == 201

    url = reverse("v0:structures-detail", kwargs={"pk": structure.id})
    response = api_client.get(url)
    resp_data = response.json()

    assert resp_data == {
        "id": ANY,
        "latest_reports": [
            {
                "id": ANY,
                "data": {
                    "id": ANY,
                    "typologie": None,
                    "structure_parente": None,
                    "nom": "Perrin",
                    "siret": "60487647500499",
                    "rna": "W382421948",
                    "presentation_resume": "Bouche valeur.",
                    "site_web": "https://www.gay.fr/",
                    "presentation_detail": "Or sans espace.",
                    "telephone": "0102030405",
                    "courriel": "frobin@example.com",
                    "code_postal": "14841",
                    "code_insee": "83989",
                    "commune": "Hebert",
                    "adresse": "avenue de Prévost",
                    "complement_adresse": "",
                    "longitude": 129.050286,
                    "latitude": -25.0098555,
                    "source": "itou",
                    "date_maj": ANY,
                    "score_geocodage": 0.5,
                    "extra": {},
                },
                "created_at": ANY,
                "updated_at": ANY,
                "antennes_data": [
                    {
                        "id": ANY,
                        "typologie": None,
                        "structure_parente": ANY,
                        "nom": "Hebert",
                        "siret": None,
                        "rna": None,
                        "presentation_resume": "",
                        "site_web": "",
                        "presentation_detail": "",
                        "telephone": "",
                        "courriel": "",
                        "code_postal": "09891",
                        "code_insee": "13991",
                        "commune": "Robinboeuf",
                        "adresse": "rue de Leclercq",
                        "complement_adresse": "",
                        "longitude": None,
                        "latitude": None,
                        "source": "itou",
                        "date_maj": "2022-04-28T18:53:11+02:00",
                        "score_geocodage": None,
                        "extra": {},
                    }
                ],
            }
        ],
        "updated_at": ANY,
        "created_at": ANY,
        "siret": "60487647500499",
        "rna": "W382421948",
    }

    # l'antenne référence la structure mère
    assert (
        resp_data["latest_reports"][0]["antennes_data"][0]["structure_parente"]
        == resp_data["latest_reports"][0]["data"]["id"]
    )
    # l'identifiant de l'antenne diffère de celui de la source
    assert resp_data["latest_reports"][0]["antennes_data"][0]["id"] != resp_data["id"]
