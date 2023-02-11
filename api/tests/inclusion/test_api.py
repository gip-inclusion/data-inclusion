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
                "id": "nom-asseoir",
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
                "date_maj": "2023-01-01",
                "antenne": False,
                "lien_source": "https://dora.fr/nom-asseoir",
                "horaires_ouverture": 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                "accessibilite": "https://acceslibre.beta.gouv.fr/app/nom-asseoir/",
                "labels_nationaux": [],
                "labels_autres": ["SudLabs", "Nièvre médiation numérique"],
                "thematiques": ["choisir-un-metier", "creation-activite"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
    }


def assert_structure_data(structure, data):
    assert structure.id == data["id"]
    assert structure.siret == data["siret"]
    assert structure.rna == data["rna"]
    assert structure.nom == data["nom"]
    assert structure.commune == data["commune"]
    assert structure.code_postal == data["code_postal"]
    assert structure.code_insee == data["code_insee"]
    assert structure.adresse == data["adresse"]
    assert structure.complement_adresse == data["complement_adresse"]
    assert structure.longitude == data["longitude"]
    assert structure.latitude == data["latitude"]
    assert structure.typologie == data["typologie"]
    assert structure.telephone == data["telephone"]
    assert structure.courriel == data["courriel"]
    assert structure.site_web == data["site_web"]
    assert structure.presentation_resume == data["presentation_resume"]
    assert structure.presentation_detail == data["presentation_detail"]
    assert structure.source == data["source"]
    assert str(structure.date_maj) == data["date_maj"]
    assert structure.antenne == data["antenne"]
    assert structure.lien_source == data["lien_source"]
    assert structure.horaires_ouverture == data["horaires_ouverture"]
    assert structure.accessibilite == data["accessibilite"]
    assert structure.labels_nationaux == data["labels_nationaux"]
    assert structure.labels_autres == data["labels_autres"]
    assert structure.thematiques == data["thematiques"]


@pytest.mark.with_token
def test_list_structures_filter_by_typology(api_client, structure_factory):
    structure_1 = structure_factory(typologie=schema.Typologie.ASSO.value)
    structure_factory(typologie=schema.Typologie.CCAS.value)

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"typologie": schema.Typologie.ASSO.value})

    resp_data = response.json()
    assert resp_data == {"items": ANY, "total": 1, "page": 1, "size": ANY}
    assert_structure_data(structure_1, resp_data["items"][0])

    response = api_client.get(url, params={"typologie": schema.Typologie.MUNI.value})
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": ANY}


@pytest.mark.with_token
def test_list_structures_filter_by_label(api_client, structure_factory):
    structure_factory(labels_nationaux=[schema.LabelNational.POLE_EMPLOI.value])
    structure_2 = structure_factory(
        labels_nationaux=[
            schema.LabelNational.MOBIN.value,
            schema.LabelNational.FRANCE_SERVICE.value,
        ]
    )

    url = "/api/v0/structures/"
    response = api_client.get(
        url, params={"label_national": schema.LabelNational.FRANCE_SERVICE.value}
    )

    resp_data = response.json()
    assert resp_data == {"items": ANY, "total": 1, "page": 1, "size": ANY}
    assert_structure_data(structure_2, resp_data["items"][0])

    response = api_client.get(
        url, params={"label_national": schema.LabelNational.AFPA.value}
    )
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": ANY}


@pytest.mark.with_token
def test_list_structures_filter_by_source(api_client, structure_factory):
    structure_1 = structure_factory(source="emplois-de-linclusion")
    structure_factory(source="dora")

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"source": "emplois-de-linclusion"})

    resp_data = response.json()
    assert resp_data == {"items": ANY, "total": 1, "page": 1, "size": ANY}
    assert_structure_data(structure_1, resp_data["items"][0])

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
    structure_1 = structure_factory(code_insee="2A247")
    structure_factory(code_insee="59350")

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"departement": "2A"})

    resp_data = response.json()
    assert resp_data == {"items": ANY, "total": 1, "page": 1, "size": ANY}
    assert_structure_data(structure_1, resp_data["items"][0])

    response = api_client.get(url, params={"departement": "62"})
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": ANY}


@pytest.mark.with_token
def test_list_structures_filter_by_departement_slug(api_client, structure_factory):
    structure_1 = structure_factory(code_insee="22247")
    structure_factory(code_insee="59350")

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"departement_slug": "cotes-d-armor"})

    resp_data = response.json()
    assert resp_data == {"items": ANY, "total": 1, "page": 1, "size": ANY}
    assert_structure_data(structure_1, resp_data["items"][0])

    response = api_client.get(url, params={"departement_slug": "pas-de-calais"})
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": ANY}


@pytest.mark.with_token
def test_list_structures_filter_by_code_postal(api_client, structure_factory):
    structure_1 = structure_factory(
        code_postal="59100", code_insee="59512", commune="roubaix"
    )
    structure_factory(code_postal="59178", code_insee="59100", commune="bousignies")

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"code_postal": "59100"})

    resp_data = response.json()
    assert resp_data == {"items": ANY, "total": 1, "page": 1, "size": ANY}
    assert_structure_data(structure_1, resp_data["items"][0])

    response = api_client.get(url, params={"code_postal": "59512"})
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": ANY}


@pytest.mark.with_token
def test_list_structures_filter_by_thematique(api_client, structure_factory):
    structure_1 = structure_factory(
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

    resp_data = response.json()
    assert resp_data == {"items": ANY, "total": 1, "page": 1, "size": ANY}
    assert_structure_data(structure_1, resp_data["items"][0])

    response = api_client.get(
        url, params={"thematique": schema.Thematique.PREPARER_SA_CANDIDATURE.value}
    )
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": ANY}


@pytest.mark.with_token
def test_list_structures_filter_by_source_and_id(api_client, structure_factory):
    structure_factory(source="emplois-de-linclusion", id="foo")
    structure_2 = structure_factory(source="dora", id="foo")
    structure_factory(source="dora", id="bar")

    url = "/api/v0/structures/"

    response = api_client.get(url, params={"source": "dora", "id": "foo"})
    resp_data = response.json()

    resp_data = response.json()
    assert resp_data == {"items": ANY, "total": 1, "page": 1, "size": ANY}
    assert_structure_data(structure_2, resp_data["items"][0])


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
                "structure_id": "prince-point-monde",
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
    structure_1 = structure_factory(source="alpha", id="2")
    structure_2 = structure_factory(source="beta", id="1")
    structure_3 = structure_factory(source="alpha", id="1")

    url = "/api/v0/structures/"

    response = api_client.get(url)

    assert response.status_code == 200

    resp_data = response.json()

    assert resp_data["items"][0]["id"] == structure_3.id
    assert resp_data["items"][1]["id"] == structure_1.id
    assert resp_data["items"][2]["id"] == structure_2.id


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


@pytest.mark.with_token
def test_list_services_filter_by_thematique(api_client, service_factory):
    service_1 = service_factory(
        source="alpha",
        id="1",
        thematiques=[
            schema.Thematique.MOBILITE.value,
            schema.Thematique.NUMERIQUE.value,
        ],
    )
    service_2 = service_factory(
        source="alpha",
        id="2",
        thematiques=[
            schema.Thematique.TROUVER_UN_EMPLOI.value,
            schema.Thematique.NUMERIQUE.value,
        ],
    )
    service_factory(thematiques=[])

    url = "/api/v0/services/"

    response = api_client.get(
        url, params={"thematique": schema.Thematique.MOBILITE.value}
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert len(resp_data["items"]) == 1
    assert resp_data["items"][0]["id"] == service_1.id
    assert resp_data["items"][0]["source"] == service_1.structure.source

    response = api_client.get(
        url, params={"thematique": schema.Thematique.NUMERIQUE.value}
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert len(resp_data["items"]) == 2
    assert resp_data["items"][0]["id"] == service_1.id
    assert resp_data["items"][0]["source"] == service_1.structure.source
    assert resp_data["items"][1]["id"] == service_2.id
    assert resp_data["items"][1]["source"] == service_2.structure.source

    response = api_client.get(
        url, params={"thematique": schema.Thematique.PREPARER_SA_CANDIDATURE.value}
    )
    assert response.status_code == 200
    assert response.json() == {"items": [], "total": 0, "page": 1, "size": ANY}
