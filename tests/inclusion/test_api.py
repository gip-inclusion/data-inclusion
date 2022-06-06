from unittest.mock import ANY

import pytest

from django.urls import reverse

pytestmark = pytest.mark.django_db


@pytest.mark.as_user
def test_create_report_nominal(api_client):
    url = reverse("v0:reports-list")
    data = {
        "typologie": None,
        "id": "60487647500499-hebert",
        "siret": "60487647500499",
        "rna": "W382421948",
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
        "lien_source": "https://dora.fr/60487647500499-hebert/",
    }
    response = api_client.post(url, data, format="json")

    assert response.status_code == 201
    assert response.json() == {
        "id": "60487647500499-hebert",
        "siret": "60487647500499",
        "rna": "W382421948",
        "typologie": None,
        "structure_parente": None,
        "lien_source": "https://dora.fr/60487647500499-hebert/",
        "updated_at": ANY,
        "created_at": ANY,
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
        "source": "dora",
        "date_maj": "2022-04-28T18:53:11+02:00",
        "score_geocodage": 0.5,
        "extra": {},
    }


@pytest.mark.as_user
def test_create_report_minimal(api_client):
    url = reverse("v0:reports-list")
    data = {
        "id": "60487647500499-hebert",
        "siret": "60487647500499",
        "nom": "Hebert",
        "code_postal": "09891",
        "code_insee": "13991",
        "commune": "Robinboeuf",
        "adresse": "rue de Leclercq",
        "date_maj": "2022-04-28T16:53:11Z",
    }
    response = api_client.post(url, data, format="json")

    assert response.status_code == 201
    assert response.json() == {
        "id": "60487647500499-hebert",
        "siret": "60487647500499",
        "rna": None,
        "typologie": None,
        "structure_parente": None,
        "lien_source": "",
        "updated_at": ANY,
        "created_at": ANY,
        "nom": "Hebert",
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
    }


@pytest.mark.as_user
def test_retrieve_report(api_client, structure_report):
    url = reverse("v0:reports-detail", kwargs={"pk": structure_report.id})
    response = api_client.get(url)
    resp_data = response.json()

    assert resp_data == {
        "id": ANY,
        "data": {
            "id": "matiere-nom-asseoir",
            "typologie": None,
            "structure_parente": None,
            "nom": "Pottier SARL",
            "siret": "76475938200654",
            "rna": "W219489241",
            "presentation_resume": "Peu répondre chant.",
            "site_web": "https://courtois.org/",
            "presentation_detail": "Or personne jambe.",
            "telephone": "0102030405",
            "courriel": "bonninveronique@example.net",
            "code_postal": "84833",
            "code_insee": "94775",
            "commune": "Sainte JulietteBourg",
            "adresse": "453, chemin Ferreira",
            "complement_adresse": "",
            "longitude": -61.64115,
            "latitude": 9.8741475,
            "source": "dora",
            "date_maj": ANY,
            "lien_source": "https://dora.fr/matiere-nom-asseoir",
            "score_geocodage": 0.5,
            "extra": {},
        },
        "created_at": ANY,
        "updated_at": ANY,
        "antennes_data": [],
    }


@pytest.mark.as_user
def test_create_antenne_report(api_client, structure_report):
    url = reverse("v0:reports-list")
    data = {
        "id": "60487647500499-hebert-1",
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

    url = reverse("v0:reports-detail", kwargs={"pk": structure_report.id})
    response = api_client.get(url)
    resp_data = response.json()

    assert resp_data == {
        "id": ANY,
        "data": {
            "id": "matiere-nom-asseoir",
            "typologie": None,
            "structure_parente": None,
            "nom": "Pottier SARL",
            "siret": "76475938200654",
            "rna": "W219489241",
            "presentation_resume": "Peu répondre chant.",
            "site_web": "https://courtois.org/",
            "presentation_detail": "Or personne jambe.",
            "telephone": "0102030405",
            "courriel": "bonninveronique@example.net",
            "code_postal": "84833",
            "code_insee": "94775",
            "commune": "Sainte JulietteBourg",
            "adresse": "453, chemin Ferreira",
            "complement_adresse": "",
            "longitude": -61.64115,
            "latitude": 9.8741475,
            "source": "itou",
            "date_maj": ANY,
            "lien_source": "https://itou.fr/matiere-nom-asseoir",
            "score_geocodage": 0.5,
            "extra": {},
        },
        "created_at": ANY,
        "updated_at": ANY,
        "antennes_data": [
            {
                "id": "60487647500499-hebert-1",
                "typologie": None,
                "structure_parente": "matiere-nom-asseoir",
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
                "date_maj": ANY,
                "lien_source": "",
                "score_geocodage": None,
                "extra": {},
            }
        ],
    }


def test_create_report_unauthenticated(api_client):
    url = reverse("v0:reports-list")
    data = {
        "siret": "60487647500499",
        "nom": "Hebert",
        "code_postal": "09891",
        "code_insee": "13991",
        "commune": "Robinboeuf",
        "adresse": "rue de Leclercq",
        "date_maj": "2022-04-28T16:53:11Z",
    }
    response = api_client.post(url, data, format="json")

    assert response.status_code == 403


def test_list_reports_unauthenticated(api_client, structure_report):
    url = reverse("v0:reports-list")
    response = api_client.get(url)

    assert response.status_code == 403


def test_retrieve_report_unauthenticated(api_client, structure_report):
    url = reverse("v0:reports-detail", kwargs={"pk": structure_report.id})
    response = api_client.get(url)

    assert response.status_code == 403


@pytest.mark.as_user
def test_list_latest_reports(api_client, structure_report_factory):
    earliest_report = structure_report_factory()
    latest_report = structure_report_factory(
        id_in_source=earliest_report.id_in_source,
        source=earliest_report.source,
    )

    url = reverse("v0:reports-list")
    response = api_client.get(url)
    resp_data = response.json()

    assert resp_data == {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "id": str(latest_report.id),
                "data": {
                    "id": "matiere-nom-asseoir",
                    "typologie": None,
                    "structure_parente": None,
                    "nom": "Ferreira",
                    "siret": "56012309800219",
                    "rna": "W101399161",
                    "presentation_resume": "Arrêter sérieux.",
                    "site_web": "http://www.voisin.com/",
                    "presentation_detail": "Membre pain second.",
                    "telephone": "0102030405",
                    "courriel": "mfournier@example.org",
                    "code_postal": "34579",
                    "code_insee": "30225",
                    "commune": "DeschampsBourg",
                    "adresse": "12, avenue Hélène Grégoire",
                    "complement_adresse": "",
                    "longitude": -12.793704,
                    "latitude": 84.196756,
                    "source": "dora",
                    "date_maj": ANY,
                    "lien_source": "https://dora.fr/matiere-nom-asseoir",
                    "score_geocodage": 0.5,
                    "extra": {},
                },
                "created_at": ANY,
                "updated_at": ANY,
                "antennes_data": [],
            }
        ],
    }
