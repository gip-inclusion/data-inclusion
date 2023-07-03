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
                "_di_geocodage_code_insee": "55626",
                "_di_geocodage_score": 0.33,
                "id": "prince-point-monde",
                "siret": "59382421200611",
                "rna": "W948924115",
                "nom": "Vaillant",
                "commune": "Sainte Bernadetteboeuf",
                "code_postal": "80571",
                "code_insee": "84442",
                "adresse": "977, rue Susan Lévy",
                "complement_adresse": None,
                "longitude": -172.461419,
                "latitude": -64.9625245,
                "typologie": "ACI",
                "telephone": "0102030405",
                "courriel": "ylacombe@example.net",
                "site_web": "http://berger.fr/",
                "presentation_resume": "Voie battre.",
                "presentation_detail": "Or personne jambe.",
                "source": "dora",
                "date_maj": "2023-01-01",
                "antenne": False,
                "lien_source": "https://dora.fr/prince-point-monde",
                "horaires_ouverture": 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                "accessibilite": "https://acceslibre.beta.gouv.fr/app/prince-point-monde/",
                "labels_nationaux": [],
                "labels_autres": ["SudLabs", "Nièvre médiation numérique"],
                "thematiques": ["choisir-un-metier", "creation-activite"],
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
        "pages": 1,
    }


def assert_paginated_response_data(
    data,
    items=ANY,
    total=ANY,
    page=ANY,
    pages=ANY,
    size=ANY,
):
    assert data == {
        "items": items,
        "total": total,
        "page": page,
        "pages": pages,
        "size": size,
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
    assert structure._di_geocodage_code_insee == data["_di_geocodage_code_insee"]
    assert structure._di_geocodage_score == data["_di_geocodage_score"]


@pytest.mark.with_token
def test_list_structures_filter_by_typology(api_client, structure_factory):
    structure_1 = structure_factory(typologie=schema.Typologie.ASSO.value)
    structure_factory(typologie=schema.Typologie.CCAS.value)

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"typologie": schema.Typologie.ASSO.value})

    resp_data = response.json()
    assert_paginated_response_data(response.json(), total=1)
    assert_structure_data(structure_1, resp_data["items"][0])

    response = api_client.get(url, params={"typologie": schema.Typologie.MUNI.value})
    assert_paginated_response_data(response.json(), total=0)


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
    assert_paginated_response_data(response.json(), total=1)
    assert_structure_data(structure_2, resp_data["items"][0])

    response = api_client.get(
        url, params={"label_national": schema.LabelNational.AFPA.value}
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_list_structures_filter_by_source(api_client, structure_factory):
    structure_1 = structure_factory(source="emplois-de-linclusion")
    structure_factory(source="dora")

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"source": "emplois-de-linclusion"})

    resp_data = response.json()
    assert_paginated_response_data(response.json(), total=1)
    assert_structure_data(structure_1, resp_data["items"][0])

    response = api_client.get(url, params={"source": "siao"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_list_sources(api_client, structure_factory):
    structure_factory(source="emplois-de-linclusion")
    structure_factory(source="dora")

    url = "/api/v0/sources/"
    response = api_client.get(url)

    assert response.json() == ["dora", "emplois-de-linclusion"]


@pytest.mark.with_token
def test_list_structures_filter_by_departement_cog(api_client, structure_factory):
    structure_1 = structure_factory(code_insee="2A247")
    structure_factory(code_insee="59350")

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"departement": "2A"})

    resp_data = response.json()
    assert_paginated_response_data(response.json(), total=1)
    assert_structure_data(structure_1, resp_data["items"][0])

    response = api_client.get(url, params={"departement": "62"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_list_structures_filter_by_departement_slug(api_client, structure_factory):
    structure_1 = structure_factory(code_insee="22247")
    structure_factory(code_insee="59350")

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"departement_slug": "cotes-d-armor"})

    resp_data = response.json()
    assert_paginated_response_data(response.json(), total=1)
    assert_structure_data(structure_1, resp_data["items"][0])

    response = api_client.get(url, params={"departement_slug": "pas-de-calais"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_list_structures_filter_by_code_postal(api_client, structure_factory):
    structure_1 = structure_factory(
        code_postal="59100", code_insee="59512", commune="roubaix"
    )
    structure_factory(code_postal="59178", code_insee="59100", commune="bousignies")

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"code_postal": "59100"})

    resp_data = response.json()
    assert_paginated_response_data(response.json(), total=1)
    assert_structure_data(structure_1, resp_data["items"][0])

    response = api_client.get(url, params={"code_postal": "59512"})
    assert_paginated_response_data(response.json(), total=0)


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
    assert_paginated_response_data(response.json(), total=1)
    assert_structure_data(structure_1, resp_data["items"][0])

    response = api_client.get(
        url, params={"thematique": schema.Thematique.PREPARER_SA_CANDIDATURE.value}
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_list_structures_filter_by_categorie_thematique(api_client, structure_factory):
    structure = structure_factory(
        thematiques=[
            schema.Thematique.MOBILITE__ACHETER_UN_VEHICULE_MOTORISE.value,
        ],
    )
    structure_factory(thematiques=[])

    url = "/api/v0/structures/"

    response = api_client.get(
        url, params={"thematique": schema.Thematique.MOBILITE.value}
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert len(resp_data["items"]) == 1
    assert resp_data["items"][0]["source"] == structure.source
    assert resp_data["items"][0]["id"] == structure.id


@pytest.mark.with_token
def test_list_structures_filter_by_source_and_id(api_client, structure_factory):
    structure_factory(source="emplois-de-linclusion", id="foo")
    structure_2 = structure_factory(source="dora", id="foo")
    structure_factory(source="dora", id="bar")

    url = "/api/v0/structures/"

    response = api_client.get(url, params={"source": "dora", "id": "foo"})
    resp_data = response.json()

    resp_data = response.json()
    assert_paginated_response_data(response.json(), total=1)
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
                "_di_geocodage_code_insee": "55626",
                "_di_geocodage_score": 0.33,
                "id": "cacher-violent",
                "structure_id": "rouge-empire",
                "source": "dora",
                "nom": "Munoz",
                "presentation_resume": "Puissant fine.",
                "presentation_detail": "Épaule élever un.",
                "types": ["formation", "numerique"],
                "thematiques": ["choisir-un-metier", "creation-activite"],
                "prise_rdv": "https://teixeira.fr/",
                "frais": ["gratuit", "gratuit-sous-conditions"],
                "frais_autres": "Camarade il.",
                "profils": ["femmes", "jeunes-16-26"],
                "pre_requis": None,
                "cumulable": False,
                "justificatifs": None,
                "formulaire_en_ligne": None,
                "commune": "Sainte Jacquelineboeuf",
                "code_postal": "25454",
                "code_insee": "32356",
                "adresse": "chemin de Ferreira",
                "complement_adresse": None,
                "longitude": -61.64115,
                "latitude": 9.8741475,
                "recurrence": None,
                "date_creation": "2022-01-01",
                "date_suspension": "2054-01-01",
                "lien_source": "https://dora.fr/cacher-violent",
                "telephone": "0102030405",
                "courriel": "xavierlaunay@example.org",
                "contact_public": False,
                "contact_nom_prenom": "David Rocher",
                "date_maj": "2023-01-01",
                "modes_accueil": ["a-distance"],
                "modes_orientation_accompagnateur": ["telephoner"],
                "modes_orientation_beneficiaire": ["telephoner"],
                "zone_diffusion_type": "commune",
                "zone_diffusion_code": "59350",
                "zone_diffusion_nom": "Lille",
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
        "pages": 1,
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
def test_list_modes_accueil(api_client):
    url = "/api/v0/doc/modes-accueil/"

    response = api_client.get(url)

    assert response.status_code == 200

    resp_data = response.json()

    assert resp_data[0] == {"value": ANY, "label": ANY, "description": ANY}


@pytest.mark.with_token
def test_list_modes_orientation_accompagnateur(api_client):
    url = "/api/v0/doc/modes-orientation-accompagnateur/"

    response = api_client.get(url)

    assert response.status_code == 200

    resp_data = response.json()

    assert resp_data[0] == {"value": ANY, "label": ANY, "description": ANY}


@pytest.mark.with_token
def test_list_modes_orientation_beneficiaire(api_client):
    url = "/api/v0/doc/modes-orientation-beneficiaire/"

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
        structure__source="alpha",
        id="1",
        thematiques=[
            schema.Thematique.MOBILITE.value,
            schema.Thematique.NUMERIQUE.value,
        ],
    )
    service_2 = service_factory(
        structure__source="alpha",
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
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_list_services_filter_by_categorie_thematique(api_client, service_factory):
    service = service_factory(
        structure__source="alpha",
        id="1",
        thematiques=[
            schema.Thematique.MOBILITE__ACHETER_UN_VEHICULE_MOTORISE.value,
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
    assert resp_data["items"][0]["source"] == service.structure.source
    assert resp_data["items"][0]["id"] == service.id


@pytest.mark.with_token
def test_list_services_filter_by_departement_cog(api_client, service_factory):
    service = service_factory(code_insee="2A247")
    service_factory(code_insee="59350")

    url = "/api/v0/services/"
    response = api_client.get(url, params={"departement": "2A"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == service.id

    response = api_client.get(url, params={"departement": "62"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_list_services_filter_by_departement_slug(api_client, service_factory):
    service = service_factory(code_insee="22247")
    service_factory(code_insee="59350")

    url = "/api/v0/services/"
    response = api_client.get(url, params={"departement_slug": "cotes-d-armor"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == service.id

    response = api_client.get(url, params={"departement_slug": "pas-de-calais"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_list_services_filter_by_code_insee(api_client, service_factory):
    service = service_factory(code_insee="22247")
    service_factory(code_insee="59350")

    url = "/api/v0/services/"
    response = api_client.get(url, params={"code_insee": "22247"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == service.id

    response = api_client.get(url, params={"code_insee": "62041"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_search_services_with_code_insee_farther_than_100km(
    api_client,
    service_factory,
    admin_express_commune_nord,
):
    # Dunkerque to Maubeuge: > 100km
    # Lille to Maubeuge: <100km
    service_1 = service_factory(
        commune="Lille", code_insee="59350", _di_geocodage_code_insee=None
    )
    service_factory(
        commune="Dunkerque", code_insee="59183", _di_geocodage_code_insee=None
    )
    service_factory(code_insee=None, _di_geocodage_code_insee=None)

    url = "/api/v0/search/services"
    response = api_client.get(
        url,
        params={
            "code_insee": "59392",  # Maubeuge
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id
    assert 0 < resp_data["items"][0]["distance"] < 100


@pytest.mark.with_token
def test_search_services_with_code_insee_geocoded(
    api_client,
    service_factory,
    admin_express_commune_nord,
):
    service_1 = service_factory(
        commune="Maubeuge", code_insee=None, _di_geocodage_code_insee="59392"
    )
    service_2 = service_factory(
        commune="Lille",
        code_insee="59350",
        _di_geocodage_code_insee="59392",  # Maubeuge
    )
    service_factory(
        commune="Dunkerque", code_insee=None, _di_geocodage_code_insee="59183"
    )
    service_factory(code_insee=None, _di_geocodage_code_insee=None)

    url = "/api/v0/search/services"
    response = api_client.get(
        url,
        params={
            "code_insee": "59392",  # Maubeuge
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert resp_data["items"][0]["service"]["id"] == service_1.id
    assert resp_data["items"][0]["distance"] == 0
    assert resp_data["items"][1]["service"]["id"] == service_2.id
    assert 0 < resp_data["items"][1]["distance"] < 100


@pytest.mark.with_token
def test_search_services_with_code_insee_ordering(
    api_client,
    service_factory,
    admin_express_commune_nord,
):
    service_1 = service_factory(code_insee="59350", _di_geocodage_code_insee=None)
    service_2 = service_factory(code_insee="59009", _di_geocodage_code_insee=None)
    service_3 = service_factory(code_insee="59183", _di_geocodage_code_insee=None)
    service_factory(code_insee=None, _di_geocodage_code_insee=None)

    url = "/api/v0/search/services"
    response = api_client.get(url, params={"code_insee": "59009"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=3)
    assert resp_data["items"][0]["service"]["id"] == service_2.id
    assert resp_data["items"][0]["distance"] == 0
    assert resp_data["items"][1]["service"]["id"] == service_1.id
    assert resp_data["items"][1]["distance"] == 0
    assert resp_data["items"][2]["service"]["id"] == service_3.id
    assert resp_data["items"][2]["distance"] > 0


@pytest.mark.with_token
def test_search_services_with_code_insee_sample_distance(api_client, service_factory):
    service_1 = service_factory(
        commune="Lille",
        code_insee="59350",
        _di_geocodage_code_insee=None,
    )
    service_factory(code_insee=None, _di_geocodage_code_insee=None)

    url = "/api/v0/search/services"
    response = api_client.get(url, params={"code_insee": "59183"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id
    assert 50 < resp_data["items"][0]["distance"] < 70


@pytest.mark.with_token
def test_search_services_with_thematique(api_client, service_factory):
    service_1 = service_factory(thematiques=[schema.Thematique.NUMERIQUE.value])
    service_2 = service_factory(thematiques=[schema.Thematique.SANTE.value])
    service_factory(thematiques=[schema.Thematique.MOBILITE.value])

    url = "/api/v0/search/services"
    response = api_client.get(
        url,
        params={
            "thematiques": [
                schema.Thematique.SANTE.value,
                schema.Thematique.NUMERIQUE.value,
            ],
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert resp_data["items"][0]["service"]["id"] in [service_1.id, service_2.id]
    assert resp_data["items"][1]["service"]["id"] in [service_1.id, service_2.id]

    response = api_client.get(
        url,
        params={
            "thematiques": schema.Thematique.CREATION_ACTIVITE.value,
        },
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_search_services_with_frais(api_client, service_factory):
    service_1 = service_factory(frais=[schema.Frais.GRATUIT.value])
    service_2 = service_factory(frais=[schema.Frais.ADHESION.value])
    service_factory(frais=[schema.Frais.PASS_NUMERIQUE.value])

    url = "/api/v0/search/services"
    response = api_client.get(
        url,
        params={
            "frais": [
                schema.Frais.GRATUIT.value,
                schema.Frais.ADHESION.value,
            ],
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert resp_data["items"][0]["service"]["id"] in [service_1.id, service_2.id]
    assert resp_data["items"][1]["service"]["id"] in [service_1.id, service_2.id]

    response = api_client.get(
        url,
        params={
            "frais": schema.Frais.PAYANT.value,
        },
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_search_services_with_types(api_client, service_factory):
    service_1 = service_factory(types=[schema.TypologieService.ACCUEIL.value])
    service_2 = service_factory(types=[schema.TypologieService.ACCOMPAGNEMENT.value])
    service_factory(types=[schema.TypologieService.AIDE_FINANCIERE.value])

    url = "/api/v0/search/services"
    response = api_client.get(
        url,
        params={
            "types": [
                schema.TypologieService.ACCUEIL.value,
                schema.TypologieService.ACCOMPAGNEMENT.value,
            ],
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert resp_data["items"][0]["service"]["id"] in [service_1.id, service_2.id]
    assert resp_data["items"][1]["service"]["id"] in [service_1.id, service_2.id]

    response = api_client.get(
        url,
        params={
            "types": schema.TypologieService.ATELIER.value,
        },
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_retrieve_service(api_client, service_factory):
    service_1 = service_factory(source="foo", id="1")
    service_2 = service_factory(source="bar", id="1")
    service_3 = service_factory(source="foo", id="2")

    url = "/api/v0/services/"
    response = api_client.get(url + f"{service_1.source}/{service_1.id}")

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data["id"] == service_1.id
    assert "structure" in resp_data
    assert resp_data["structure"]["id"] == service_1.structure.id

    response = api_client.get(url + f"{service_2.source}/{service_3.id}")
    assert response.status_code == 404


@pytest.mark.with_token
def test_retrieve_structure(api_client, structure_factory, service_factory):
    structure_1 = structure_factory(source="foo", id="1")
    service_1 = service_factory(structure=structure_1)
    structure_2 = structure_factory(source="bar", id="1")
    service_factory(structure=structure_2)
    structure_3 = structure_factory(source="foo", id="2")

    url = "/api/v0/structures/"
    response = api_client.get(url + f"{structure_1.source}/{structure_1.id}")

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data["id"] == structure_1.id
    assert "services" in resp_data
    assert len(resp_data["services"]) == 1
    assert resp_data["services"][0]["id"] == service_1.id

    response = api_client.get(url + f"{structure_2.source}/{structure_3.id}")
    assert response.status_code == 404
