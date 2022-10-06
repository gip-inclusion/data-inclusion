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
                "source": "itou",
                "date_maj": ANY,
                "structure_parente": None,
                "lien_source": "https://itou.fr/matiere-nom-asseoir",
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
                "thematiques": ["preparer-sa-candidature", "trouver-un-emploi"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
    }

    response = api_client.get(
        url, params={"label_national": schema.LabelNational.AFPA.value}
    )
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": 50}


@pytest.mark.with_token
def test_list_structures_filter_by_source(api_client, structure_factory):
    structure_factory(source="itou")
    structure_factory(source="dora")

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"source": "itou"})

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
                "typologie": "AI",
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
                "thematiques": ["choisir-un-metier", "creation-activite"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
    }

    response = api_client.get(url, params={"source": "siao"})
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": 50}


@pytest.mark.with_token
def test_list_sources(api_client, structure_factory):
    structure_factory(source="itou")
    structure_factory(source="dora")

    url = "/api/v0/sources/"
    response = api_client.get(url)

    assert response.json() == ["dora", "itou"]


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
                "typologie": "Autre",
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
                "thematiques": ["preparer-sa-candidature", "trouver-un-emploi"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
    }
    response = api_client.get(url, params={"departement": "62"})
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": 50}


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
                "typologie": "CAARUD",
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
                "thematiques": ["creation-activite", "mobilite"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
    }

    response = api_client.get(url, params={"departement_slug": "pas-de-calais"})
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": 50}


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
                "typologie": "CAF",
                "telephone": "0102030405",
                "courriel": "qdijoux@example.org",
                "site_web": "http://www.lacombe.com/",
                "presentation_resume": "Signer possible.",
                "presentation_detail": "Revenir honte avis ensemble.",
                "source": "itou",
                "date_maj": ANY,
                "structure_parente": None,
                "lien_source": "https://itou.fr/matiere-nom-asseoir",
                "horaires_ouverture": 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                "accessibilite": "https://acceslibre.beta.gouv.fr/app/matiere-nom-asseoir/",
                "labels_nationaux": [],
                "labels_autres": ["SudLabs", "Nièvre médiation numérique"],
                "thematiques": ["numerique", "preparer-sa-candidature"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
    }

    response = api_client.get(url, params={"code_postal": "59512"})
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": 50}


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
                "typologie": "CAVA",
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
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": 50}


@pytest.mark.with_token
def test_list_structures_filter_by_source_and_id(api_client, structure_factory):
    structure_factory(source="itou", id="foo")
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
                "typologie": "CD",
                "telephone": "0102030405",
                "courriel": "xrobin@example.org",
                "site_web": "http://www.gonzalez.fr/",
                "presentation_resume": "Écraser bas un an.",
                "presentation_detail": "Lieu apparence bon voir.",
                "source": "dora",
                "date_maj": ANY,
                "structure_parente": None,
                "lien_source": "https://dora.fr/foo",
                "horaires_ouverture": 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                "accessibilite": "https://acceslibre.beta.gouv.fr/app/foo/",
                "labels_nationaux": [],
                "labels_autres": ["SudLabs", "Nièvre médiation numérique"],
                "thematiques": ["creation-activite", "mobilite"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": 50,
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
                "source": "dora",
                "structure_id": "matiere-nom-asseoir",
                "nom": "Munoz",
                "presentation_resume": "Puissant fine.",
                "types": ["accompagnement", "accueil"],
                "thematiques": ["choisir-un-metier", "creation-activite"],
                "prise_rdv": "http://www.delorme.org/",
                "frais": ["gratuit", "gratuit-sous-conditions"],
                "frais_autres": "Parmi position ton.",
            }
        ],
        "total": 1,
        "page": 1,
        "size": 50,
    }
