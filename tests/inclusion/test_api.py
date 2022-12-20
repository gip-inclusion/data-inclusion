from datetime import datetime, timedelta
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
                "antenne": False,
                "lien_source": "https://dora.fr/matiere-nom-asseoir",
                "horaires_ouverture": 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                "accessibilite": "https://acceslibre.beta.gouv.fr/app/matiere-nom-asseoir/",
                "labels_nationaux": [],
                "labels_autres": ["SudLabs", "Nièvre médiation numérique"],
                "thematiques": ["choisir-un-metier", "creation-activite"],
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
                "source": "emplois-de-linclusion",
                "date_maj": ANY,
                "antenne": False,
                "lien_source": "https://emplois-de-linclusion.fr/matiere-nom-asseoir",
                "horaires_ouverture": 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                "accessibilite": "https://acceslibre.beta.gouv.fr/app/matiere-nom-asseoir/",
                "labels_nationaux": [],
                "labels_autres": ["SudLabs", "Nièvre médiation numérique"],
                "thematiques": ["creation-activite", "mobilite"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
    }

    response = api_client.get(url, params={"typologie": schema.Typologie.MUNI.value})
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": ANY}


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
                "typologie": "PE",
                "telephone": "0102030405",
                "courriel": "xrobin@example.org",
                "site_web": "http://www.gonzalez.fr/",
                "presentation_resume": "Écraser bas un an.",
                "presentation_detail": "Lieu apparence bon voir.",
                "source": "dora",
                "date_maj": ANY,
                "antenne": False,
                "lien_source": "https://dora.fr/cacher-violent",
                "horaires_ouverture": 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                "accessibilite": "https://acceslibre.beta.gouv.fr/app/cacher-violent/",
                "labels_nationaux": ["mobin", "france-service"],
                "labels_autres": ["SudLabs", "Nièvre médiation numérique"],
                "thematiques": ["choisir-un-metier", "creation-activite"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
    }

    response = api_client.get(
        url, params={"label_national": schema.LabelNational.AFPA.value}
    )
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": ANY}


@pytest.mark.with_token
def test_list_structures_filter_by_source(api_client, structure_factory):
    structure_factory(source="emplois-de-linclusion")
    structure_factory(source="dora")

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"source": "emplois-de-linclusion"})

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
                "typologie": "ACI",
                "telephone": "0102030405",
                "courriel": "raymondclemence@example.com",
                "site_web": "http://aubert.net/",
                "presentation_resume": "Espèce couler.",
                "presentation_detail": "Or personne jambe.",
                "source": "emplois-de-linclusion",
                "date_maj": ANY,
                "antenne": False,
                "lien_source": "https://emplois-de-linclusion.fr/matiere-nom-asseoir",
                "horaires_ouverture": 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                "accessibilite": "https://acceslibre.beta.gouv.fr/app/matiere-nom-asseoir/",
                "labels_nationaux": [],
                "labels_autres": ["SudLabs", "Nièvre médiation numérique"],
                "thematiques": ["creation-activite", "mobilite"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
    }

    response = api_client.get(url, params={"source": "siao"})
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": ANY}


@pytest.mark.with_token
def test_list_sources(api_client, structure_factory):
    structure_factory(source="emplois-de-linclusion")
    structure_factory(source="dora")

    url = "/api/v0/sources/"
    response = api_client.get(url)

    assert response.json() == ["emplois-de-linclusion", "dora"]


@pytest.mark.with_token
def test_list_structures_filter_by_departement_cog(api_client, structure_factory):
    structure_factory(code_insee="2A247")
    structure_factory(code_insee="59350")

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"departement": "2A"})

    assert response.json() == {
        "items": [
            {
                "id": "matiere-nom-asseoir",
                "siret": "76475938200654",
                "rna": "W219489241",
                "nom": "Pottier SARL",
                "commune": "VaillantBourg",
                "code_postal": "65938",
                "code_insee": "2A247",
                "adresse": "58, boulevard Garcia",
                "complement_adresse": None,
                "longitude": -129.925049,
                "latitude": 17.058379,
                "typologie": "MUNI",
                "telephone": "0102030405",
                "courriel": "raymondclemence@example.com",
                "site_web": "http://aubert.net/",
                "presentation_resume": "Espèce couler.",
                "presentation_detail": "Or personne jambe.",
                "source": "emplois-de-linclusion",
                "date_maj": ANY,
                "antenne": False,
                "lien_source": "https://emplois-de-linclusion.fr/matiere-nom-asseoir",
                "horaires_ouverture": 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                "accessibilite": "https://acceslibre.beta.gouv.fr/app/matiere-nom-asseoir/",
                "labels_nationaux": [],
                "labels_autres": ["SudLabs", "Nièvre médiation numérique"],
                "thematiques": ["creation-activite", "mobilite"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
    }
    response = api_client.get(url, params={"departement": "62"})
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": ANY}


@pytest.mark.with_token
def test_list_structures_filter_by_departement_slug(api_client, structure_factory):
    structure_factory(code_insee="22247")
    structure_factory(code_insee="59350")

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"departement_slug": "cotes-d-armor"})

    assert response.json() == {
        "items": [
            {
                "id": "matiere-nom-asseoir",
                "siret": "76475938200654",
                "rna": "W219489241",
                "nom": "Pottier SARL",
                "commune": "VaillantBourg",
                "code_postal": "65938",
                "code_insee": "22247",
                "adresse": "58, boulevard Garcia",
                "complement_adresse": None,
                "longitude": -129.925049,
                "latitude": 17.058379,
                "typologie": "ACI",
                "telephone": "0102030405",
                "courriel": "raymondclemence@example.com",
                "site_web": "http://aubert.net/",
                "presentation_resume": "Espèce couler.",
                "presentation_detail": "Or personne jambe.",
                "source": "emplois-de-linclusion",
                "date_maj": ANY,
                "antenne": False,
                "lien_source": "https://emplois-de-linclusion.fr/matiere-nom-asseoir",
                "horaires_ouverture": 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                "accessibilite": "https://acceslibre.beta.gouv.fr/app/matiere-nom-asseoir/",
                "labels_nationaux": [],
                "labels_autres": ["SudLabs", "Nièvre médiation numérique"],
                "thematiques": ["creation-activite", "mobilite"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
    }

    response = api_client.get(url, params={"departement_slug": "pas-de-calais"})
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": ANY}


@pytest.mark.with_token
def test_list_structures_filter_by_code_postal(api_client, structure_factory):
    structure_factory(code_postal="59100", code_insee="59512", commune="roubaix")
    structure_factory(code_postal="59178", code_insee="59100", commune="bousignies")

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"code_postal": "59100"})

    assert response.json() == {
        "items": [
            {
                "id": "matiere-nom-asseoir",
                "siret": "76475938200654",
                "rna": "W219489241",
                "nom": "Pottier SARL",
                "commune": "roubaix",
                "code_postal": "59100",
                "code_insee": "59512",
                "adresse": "715, boulevard Lucie Martins",
                "complement_adresse": None,
                "longitude": 116.633888,
                "latitude": 38.046336,
                "typologie": "PE",
                "telephone": "0102030405",
                "courriel": "qdijoux@example.org",
                "site_web": "http://www.lacombe.com/",
                "presentation_resume": "Signer possible.",
                "presentation_detail": "Revenir honte avis ensemble.",
                "source": "emplois-de-linclusion",
                "date_maj": ANY,
                "antenne": False,
                "lien_source": "https://emplois-de-linclusion.fr/matiere-nom-asseoir",
                "horaires_ouverture": 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                "accessibilite": "https://acceslibre.beta.gouv.fr/app/matiere-nom-asseoir/",
                "labels_nationaux": [],
                "labels_autres": ["SudLabs", "Nièvre médiation numérique"],
                "thematiques": ["creation-activite", "mobilite"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
    }

    response = api_client.get(url, params={"code_postal": "59512"})
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": ANY}


@pytest.mark.with_token
def test_list_structures_filter_by_thematique(api_client, structure_factory):
    structure_factory(
        thematiques=[
            schema.Thematique.MOBILITE.value,
            schema.Thematique.NUMERIQUE.value,
        ]
    )
    structure_factory(
        thematiques=[
            schema.Thematique.TROUVER_UN_EMPLOI.value,
            schema.Thematique.NUMERIQUE.value,
        ]
    )
    structure_factory(thematiques=[])

    url = "/api/v0/structures/"
    response = api_client.get(
        url, params={"thematique": schema.Thematique.MOBILITE.value}
    )

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
                "typologie": "MUNI",
                "telephone": "0102030405",
                "courriel": "raymondclemence@example.com",
                "site_web": "http://aubert.net/",
                "presentation_resume": "Espèce couler.",
                "presentation_detail": "Or personne jambe.",
                "source": "emplois-de-linclusion",
                "date_maj": ANY,
                "antenne": False,
                "lien_source": "https://emplois-de-linclusion.fr/matiere-nom-asseoir",
                "horaires_ouverture": 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                "accessibilite": "https://acceslibre.beta.gouv.fr/app/matiere-nom-asseoir/",
                "labels_nationaux": [],
                "labels_autres": ["SudLabs", "Nièvre médiation numérique"],
                "thematiques": ["mobilite", "numerique"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
    }

    response = api_client.get(
        url, params={"thematique": schema.Thematique.PREPARER_SA_CANDIDATURE.value}
    )
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": ANY}


@pytest.mark.with_token
def test_list_structures_filter_by_source_and_id(api_client, structure_factory):
    structure_factory(source="emplois-de-linclusion", id="foo")
    structure_factory(source="dora", id="foo")
    structure_factory(source="dora", id="bar")

    url = "/api/v0/structures/"

    response = api_client.get(url, params={"source": "dora", "id": "foo"})
    resp_data = response.json()

    assert resp_data == {
        "items": [
            {
                "id": "foo",
                "siret": "86848339600109",
                "rna": "W477515917",
                "nom": "Aubert",
                "commune": "Durand",
                "code_postal": "13525",
                "code_insee": "01230",
                "adresse": "191, rue Seguin",
                "complement_adresse": None,
                "longitude": 129.212387,
                "latitude": -57.869491,
                "typologie": "PE",
                "telephone": "0102030405",
                "courriel": "xrobin@example.org",
                "site_web": "http://www.gonzalez.fr/",
                "presentation_resume": "Écraser bas un an.",
                "presentation_detail": "Lieu apparence bon voir.",
                "source": "dora",
                "date_maj": ANY,
                "antenne": False,
                "lien_source": "https://dora.fr/foo",
                "horaires_ouverture": 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                "accessibilite": "https://acceslibre.beta.gouv.fr/app/foo/",
                "labels_nationaux": [],
                "labels_autres": ["SudLabs", "Nièvre médiation numérique"],
                "thematiques": ["choisir-un-metier", "creation-activite"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
    }


def test_list_services_unauthenticated(api_client):
    url = "/api/v0/services/"
    response = api_client.get(url)

    assert response.status_code == 403


@pytest.mark.with_token
def test_list_services_all(api_client, service_factory):
    service_factory()

    url = "/api/v0/services/"

    response = api_client.get(url)
    resp_data = response.json()

    assert resp_data == {
        "items": [
            {
                "id": "cacher-violent",
                "structure_id": "matiere-nom-asseoir",
                "source": "dora",
                "nom": "Munoz",
                "presentation_resume": "Puissant fine.",
                "types": ["formation", "numerique"],
                "thematiques": ["choisir-un-metier", "creation-activite"],
                "prise_rdv": "http://www.delorme.org/",
                "frais": ["gratuit", "gratuit-sous-conditions"],
                "frais_autres": "Parmi position ton.",
                "profils": ["femmes", "jeunes-16-26"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
    }


@pytest.mark.with_token
def test_list_labels_nationaux(api_client):
    url = "/api/v0/doc/labels-nationaux/"

    response = api_client.get(url)

    assert response.status_code == 200

    resp_data = response.json()

    assert resp_data[0] == {"value": ANY, "label": ANY, "description": ANY}


@pytest.mark.with_token
def test_list_thematiques(api_client):
    url = "/api/v0/doc/thematiques/"

    response = api_client.get(url)

    assert response.status_code == 200

    resp_data = response.json()

    assert resp_data[0] == {"value": ANY, "label": ANY, "description": ANY}


@pytest.mark.with_token
def test_list_typologies_services(api_client):
    url = "/api/v0/doc/typologies-services/"

    response = api_client.get(url)

    assert response.status_code == 200

    resp_data = response.json()

    assert resp_data[0] == {"value": ANY, "label": ANY, "description": ANY}


@pytest.mark.with_token
def test_list_frais(api_client):
    url = "/api/v0/doc/frais/"

    response = api_client.get(url)

    assert response.status_code == 200

    resp_data = response.json()

    assert resp_data[0] == {"value": ANY, "label": ANY, "description": ANY}


@pytest.mark.with_token
def test_list_profils(api_client):
    url = "/api/v0/doc/profils/"

    response = api_client.get(url)

    assert response.status_code == 200

    resp_data = response.json()

    assert resp_data[0] == {"value": ANY, "label": ANY, "description": ANY}


@pytest.mark.with_token
def test_list_typologies_structures(api_client):
    url = "/api/v0/doc/typologies-structures/"

    response = api_client.get(url)

    assert response.status_code == 200

    resp_data = response.json()

    assert resp_data[0] == {"value": ANY, "label": ANY, "description": ANY}


@pytest.mark.with_token
def test_list_structures_null_siret(api_client, structure_factory):
    structure = structure_factory(siret=None)

    url = "/api/v0/structures/"

    response = api_client.get(url)

    assert response.status_code == 200

    resp_data = response.json()

    assert resp_data["items"][0]["id"] == structure.id
    assert resp_data["items"][0]["siret"] is None


@pytest.mark.with_token
def test_list_structures_null_code_insee(api_client, structure_factory):
    structure = structure_factory(code_insee=None)

    url = "/api/v0/structures/"

    response = api_client.get(url)

    assert response.status_code == 200

    resp_data = response.json()

    assert resp_data["items"][0]["id"] == structure.id
    assert resp_data["items"][0]["code_insee"] is None


@pytest.mark.with_token
def test_list_structures_null_code_insee_filter_by_departement_cog(
    api_client, structure_factory
):
    structure_factory(code_insee=None)
    structure = structure_factory(code_insee="2A247")

    url = "/api/v0/structures/"

    response = api_client.get(url, params={"departement": "2A"})

    assert response.status_code == 200

    resp_data = response.json()

    assert len(resp_data["items"]) == 1
    assert resp_data["items"][0]["id"] == structure.id


@pytest.mark.with_token
def test_list_structures_null_code_insee_filter_by_departement_slug(
    api_client, structure_factory
):
    structure_factory(code_insee=None)
    structure = structure_factory(code_insee="22247")

    url = "/api/v0/structures/"

    response = api_client.get(url, params={"departement_slug": "cotes-d-armor"})

    assert response.status_code == 200

    resp_data = response.json()

    assert len(resp_data["items"]) == 1
    assert resp_data["items"][0]["id"] == structure.id


@pytest.mark.with_token
def test_list_structures_order(api_client, structure_factory):
    today_dt = datetime.now()
    yesterday_dt = datetime.now() - timedelta(days=1)

    structure_1 = structure_factory(created_at=yesterday_dt, source="alpha", id="2")
    structure_2 = structure_factory(created_at=yesterday_dt, source="alpha", id="3")
    structure_3 = structure_factory(created_at=yesterday_dt, source="beta", id="1")
    structure_4 = structure_factory(created_at=today_dt, source="alpha", id="1")

    url = "/api/v0/structures/"

    response = api_client.get(url)

    assert response.status_code == 200

    resp_data = response.json()

    assert resp_data["items"][0]["id"] == structure_1.id
    assert resp_data["items"][1]["id"] == structure_2.id
    assert resp_data["items"][2]["id"] == structure_3.id
    assert resp_data["items"][3]["id"] == structure_4.id


@pytest.mark.with_token
def test_list_services_filter_by_source(api_client, service_factory):
    service_1 = service_factory(structure__source="emplois-de-linclusion")
    service_factory(structure__source="dora")

    url = "/api/v0/services/"
    response = api_client.get(url, params={"source": "emplois-de-linclusion"})

    assert response.status_code == 200

    resp_data = response.json()

    assert len(resp_data["items"]) == 1
    assert resp_data["items"][0]["id"] == service_1.id
    assert resp_data["items"][0]["source"] == service_1.structure.source
