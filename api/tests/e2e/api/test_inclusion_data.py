import json
from datetime import date, timedelta
from unittest.mock import ANY

import pytest

from data_inclusion import schema
from data_inclusion.api.utils import soliguide

from ... import factories


def test_openapi_spec(api_client, snapshot):
    url = "/api/openapi.json"
    response = api_client.get(url)

    assert response.status_code == 200
    assert json.dumps(response.json(), indent=2, ensure_ascii=False) == snapshot


def test_list_structures_unauthenticated(api_client):
    url = "/api/v0/structures/"
    response = api_client.get(url)

    assert response.status_code == 403


@pytest.mark.with_token
def test_list_structures_all(api_client):
    factories.StructureFactory()

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
    assert float(structure.longitude) == data["longitude"]
    assert float(structure.latitude) == data["latitude"]
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
def test_list_structures_filter_by_typology(api_client):
    structure_1 = factories.StructureFactory(typologie=schema.Typologie.ASSO.value)
    factories.StructureFactory(typologie=schema.Typologie.CCAS.value)

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"typologie": schema.Typologie.ASSO.value})

    resp_data = response.json()
    assert_paginated_response_data(response.json(), total=1)
    assert_structure_data(structure_1, resp_data["items"][0])

    response = api_client.get(url, params={"typologie": schema.Typologie.MUNI.value})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_list_structures_filter_by_label(
    api_client,
):
    factories.StructureFactory(
        labels_nationaux=[schema.LabelNational.POLE_EMPLOI.value]
    )
    structure_2 = factories.StructureFactory(
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
def test_list_structures_filter_by_source(api_client):
    structure_1 = factories.StructureFactory(source="emplois-de-linclusion")
    factories.StructureFactory(source="dora")

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"source": "emplois-de-linclusion"})

    resp_data = response.json()
    assert_paginated_response_data(response.json(), total=1)
    assert_structure_data(structure_1, resp_data["items"][0])

    response = api_client.get(url, params={"source": "siao"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_list_sources(api_client):
    url = "/api/v0/sources/"
    response = api_client.get(url)

    resp_data = response.json()
    assert resp_data != []
    assert resp_data[0] == {"slug": ANY, "nom": ANY, "description": ANY}
    assert all(
        slug in [d["slug"] for d in resp_data]
        for slug in [
            "dora",
            "emplois-de-linclusion",
            "mediation-numerique",
        ]
    )
    assert not any(
        slug in [d["slug"] for d in resp_data]
        for slug in [
            "soliguide",
            "data-inclusion",
        ]
    )


@pytest.mark.with_token
def test_list_structures_filter_by_departement_cog(api_client):
    structure_1 = factories.StructureFactory(code_insee="2A247")
    factories.StructureFactory(code_insee="59350")

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"departement": "2A"})

    resp_data = response.json()
    assert_paginated_response_data(response.json(), total=1)
    assert_structure_data(structure_1, resp_data["items"][0])

    response = api_client.get(url, params={"departement": "62"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_list_structures_filter_by_departement_slug(
    api_client,
):
    structure_1 = factories.StructureFactory(code_insee="22247")
    factories.StructureFactory(code_insee="59350")

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"departement_slug": "cotes-d-armor"})

    resp_data = response.json()
    assert_paginated_response_data(response.json(), total=1)
    assert_structure_data(structure_1, resp_data["items"][0])

    response = api_client.get(url, params={"departement_slug": "pas-de-calais"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_list_structures_filter_by_code_postal(
    api_client,
):
    structure_1 = factories.StructureFactory(
        code_postal="59100", code_insee="59512", commune="roubaix"
    )
    factories.StructureFactory(
        code_postal="59178", code_insee="59100", commune="bousignies"
    )

    url = "/api/v0/structures/"
    response = api_client.get(url, params={"code_postal": "59100"})

    resp_data = response.json()
    assert_paginated_response_data(response.json(), total=1)
    assert_structure_data(structure_1, resp_data["items"][0])

    response = api_client.get(url, params={"code_postal": "59512"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_list_structures_filter_by_thematique(
    api_client,
):
    structure_1 = factories.StructureFactory(
        thematiques=[
            schema.Thematique.MOBILITE.value,
            schema.Thematique.NUMERIQUE.value,
        ]
    )
    factories.StructureFactory(
        thematiques=[
            schema.Thematique.TROUVER_UN_EMPLOI.value,
            schema.Thematique.NUMERIQUE.value,
        ]
    )
    factories.StructureFactory(thematiques=[])

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
def test_list_structures_filter_by_categorie_thematique(
    api_client,
):
    structure = factories.StructureFactory(
        thematiques=[
            schema.Thematique.MOBILITE__ACHETER_UN_VEHICULE_MOTORISE.value,
        ],
    )
    factories.StructureFactory(thematiques=[])

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
def test_list_structures_filter_by_source_and_id(
    api_client,
):
    factories.StructureFactory(source="emplois-de-linclusion", id="foo")
    structure_2 = factories.StructureFactory(source="dora", id="foo")
    factories.StructureFactory(source="dora", id="bar")

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
def test_list_services_all(api_client):
    factories.ServiceFactory()

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
                "pre_requis": [],
                "cumulable": False,
                "justificatifs": [],
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
                "modes_orientation_accompagnateur_autres": None,
                "modes_orientation_beneficiaire": ["telephoner"],
                "modes_orientation_beneficiaire_autres": None,
                "zone_diffusion_type": None,
                "zone_diffusion_code": None,
                "zone_diffusion_nom": None,
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
        "pages": 1,
    }


@pytest.mark.with_token
def test_list_structures_null_siret(
    api_client,
):
    structure = factories.StructureFactory(siret=None)

    url = "/api/v0/structures/"

    response = api_client.get(url)

    assert response.status_code == 200

    resp_data = response.json()

    assert resp_data["items"][0]["id"] == structure.id
    assert resp_data["items"][0]["siret"] is None


@pytest.mark.with_token
def test_list_structures_null_code_insee(
    api_client,
):
    structure = factories.StructureFactory(code_insee=None)

    url = "/api/v0/structures/"

    response = api_client.get(url)

    assert response.status_code == 200

    resp_data = response.json()

    assert resp_data["items"][0]["id"] == structure.id
    assert resp_data["items"][0]["code_insee"] is None


@pytest.mark.with_token
def test_list_structures_null_code_insee_filter_by_departement_cog(api_client):
    factories.StructureFactory(code_insee=None)
    structure = factories.StructureFactory(code_insee="2A247")

    url = "/api/v0/structures/"

    response = api_client.get(url, params={"departement": "2A"})

    assert response.status_code == 200

    resp_data = response.json()

    assert len(resp_data["items"]) == 1
    assert resp_data["items"][0]["id"] == structure.id


@pytest.mark.with_token
def test_list_structures_null_code_insee_filter_by_departement_slug(api_client):
    factories.StructureFactory(code_insee=None)
    structure = factories.StructureFactory(code_insee="22247")

    url = "/api/v0/structures/"

    response = api_client.get(url, params={"departement_slug": "cotes-d-armor"})

    assert response.status_code == 200

    resp_data = response.json()

    assert len(resp_data["items"]) == 1
    assert resp_data["items"][0]["id"] == structure.id


@pytest.mark.with_token
def test_list_structures_order(
    api_client,
):
    structure_1 = factories.StructureFactory(source="alpha", id="2")
    structure_2 = factories.StructureFactory(source="beta", id="1")
    structure_3 = factories.StructureFactory(source="alpha", id="1")

    url = "/api/v0/structures/"

    response = api_client.get(url)

    assert response.status_code == 200

    resp_data = response.json()

    assert resp_data["items"][0]["id"] == structure_3.id
    assert resp_data["items"][1]["id"] == structure_1.id
    assert resp_data["items"][2]["id"] == structure_2.id


@pytest.mark.with_token
def test_list_services_filter_by_source(api_client):
    service_1 = factories.ServiceFactory(structure__source="emplois-de-linclusion")
    factories.ServiceFactory(structure__source="dora")

    url = "/api/v0/services/"
    response = api_client.get(url, params={"source": "emplois-de-linclusion"})

    assert response.status_code == 200

    resp_data = response.json()

    assert len(resp_data["items"]) == 1
    assert resp_data["items"][0]["id"] == service_1.id
    assert resp_data["items"][0]["source"] == service_1.structure.source


@pytest.mark.with_token
def test_list_services_filter_by_thematique(api_client):
    service_1 = factories.ServiceFactory(
        structure__source="alpha",
        id="1",
        thematiques=[
            schema.Thematique.MOBILITE.value,
            schema.Thematique.NUMERIQUE.value,
        ],
    )
    service_2 = factories.ServiceFactory(
        structure__source="alpha",
        id="2",
        thematiques=[
            schema.Thematique.TROUVER_UN_EMPLOI.value,
            schema.Thematique.NUMERIQUE.value,
        ],
    )
    factories.ServiceFactory(thematiques=[])

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
def test_list_services_filter_by_categorie_thematique(api_client):
    service = factories.ServiceFactory(
        structure__source="alpha",
        id="1",
        thematiques=[
            schema.Thematique.MOBILITE__ACHETER_UN_VEHICULE_MOTORISE.value,
        ],
    )
    factories.ServiceFactory(thematiques=[])

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
def test_list_services_filter_by_departement_cog(api_client):
    service = factories.ServiceFactory(code_insee="2A247")
    factories.ServiceFactory(code_insee="59350")

    url = "/api/v0/services/"
    response = api_client.get(url, params={"departement": "2A"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == service.id

    response = api_client.get(url, params={"departement": "62"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_list_services_filter_by_departement_slug(api_client):
    service = factories.ServiceFactory(code_insee="22247")
    factories.ServiceFactory(code_insee="59350")

    url = "/api/v0/services/"
    response = api_client.get(url, params={"departement_slug": "cotes-d-armor"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == service.id

    response = api_client.get(url, params={"departement_slug": "pas-de-calais"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.parametrize(
    "code_insee, input, found",
    [
        (None, "22247", False),
        ("22247", "22247", True),
        ("22247", "62041", False),
        ("75056", "75101", True),
    ],
)
@pytest.mark.with_token
def test_list_services_filter_by_code_insee(api_client, code_insee, input, found):
    service = factories.ServiceFactory(code_insee=code_insee)
    factories.ServiceFactory(code_insee="59350")

    url = "/api/v0/services/"
    response = api_client.get(url, params={"code_insee": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert resp_data["items"][0]["id"] == service.id
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.parametrize(
    "code_insee, input, found",
    [
        (None, "59183", True),
        ("59183", "59183", True),
        ("59183", "59392", False),
        ("75056", "75101", True),
        ("75101", "75101", True),
        pytest.param("75101", "75056", True, marks=pytest.mark.xfail),  # TODO
    ],
)
@pytest.mark.with_token
def test_search_services_with_code_insee(api_client, code_insee, input, found):
    factories.CommuneFactory(code="59350", nom="Lille")
    factories.CommuneFactory(code="59183", nom="Dunkerque")
    factories.CommuneFactory(code="59392", nom="Maubeuge")
    factories.CommuneFactory(code="75056", nom="Paris")
    service = factories.ServiceFactory(code_insee=code_insee)

    url = "/api/v0/search/services"
    response = api_client.get(url, params={"code_insee": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert resp_data["items"][0]["service"]["id"] == service.id
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.with_token
def test_search_services_with_code_insee_too_far(api_client, generate_communes_nord):
    # Dunkerque to Hazebrouck: <50km
    # Hazebrouck to Lille: <50km
    # Dunkerque to Lille: >50km
    service_1 = factories.ServiceFactory(
        commune="Lille",
        code_insee="59350",
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
    )
    service_2 = factories.ServiceFactory(
        commune="Dunkerque",
        code_insee="59183",
        latitude=51.0361,
        longitude=2.3770,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
    )
    factories.ServiceFactory(
        code_insee=None,
        latitude=None,
        longitude=None,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
    )

    url = "/api/v0/search/services"
    response = api_client.get(
        url,
        params={
            "code_insee": "59512",  # Roubaix (only close to Lille)
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id
    assert 0 < resp_data["items"][0]["distance"] < 50

    # Do the same but with lat/lon in addition to the code INSEE
    response = api_client.get(
        url,
        params={
            "code_insee": "59512",  # Roubaix
            # Coordinates for Le Mans. We don't enforce lat/lon to be within
            # the supplied 'code_insee' city limits.
            "lat": 48.003954,
            "lon": 0.199134,
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=0)

    # This time with coordinates close to the services we know
    response = api_client.get(
        url,
        params={
            "code_insee": "59512",  # Roubaix
            # Coordinates for Hazebrouck, between Dunkirk & Lille
            "lat": 50.7262,
            "lon": 2.5387,
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    # Lille and Hazebrouck are less than 50km apart.
    assert_paginated_response_data(resp_data, total=2)
    assert resp_data["items"][0]["service"]["id"] == service_2.id
    assert 0 < resp_data["items"][0]["distance"] < 50

    # What about a request without code_insee but with lat/lon?
    response = api_client.get(
        url,
        params={
            # Coordinates for Le Mans, should be ignored.
            # the supplied 'code_insee' city limits.
            "lat": 48.003954,
            "lon": 0.199134,
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=3)

    # Error cases
    response = api_client.get(
        url,
        params={
            "code_insee": "59392",
            "lat": 48.003954,
        },
    )

    assert response.status_code == 422
    resp_data = response.json()
    assert resp_data["detail"] == "The `lat` and `lon` must be simultaneously filled."

    response = api_client.get(
        url,
        params={
            "code_insee": "59392",
            "lon": 1.2563,
        },
    )

    assert response.status_code == 422
    resp_data = response.json()
    assert resp_data["detail"] == "The `lat` and `lon` must be simultaneously filled."


@pytest.mark.with_token
def test_search_services_with_zone_diffusion_pays(api_client, generate_communes_nord):
    service_1 = factories.ServiceFactory(
        commune="Dunkerque",
        code_insee="59183",
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[schema.ModeAccueil.A_DISTANCE.value],
        zone_diffusion_type=schema.ZoneDiffusionType.PAYS.value,
        zone_diffusion_code=None,
        zone_diffusion_nom=None,
    )

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


@pytest.mark.with_token
def test_search_services_with_zone_diffusion_commune(
    api_client, generate_communes_nord
):
    service_1 = factories.ServiceFactory(
        commune="Dunkerque",
        code_insee="59183",
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=schema.ZoneDiffusionType.COMMUNE.value,
        zone_diffusion_code="59183",
        zone_diffusion_nom="Dunkerque",
    )
    factories.ServiceFactory(
        commune="Lille",
        code_insee="59350",
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=schema.ZoneDiffusionType.COMMUNE.value,
        zone_diffusion_code="59350",
        zone_diffusion_nom="Lille",
    )

    url = "/api/v0/search/services"
    response = api_client.get(
        url,
        params={
            "code_insee": "59183",  # Dunkerque
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.with_token
def test_search_services_with_zone_diffusion_epci(
    api_client,
    generate_communes_nord,
):
    service_1 = factories.ServiceFactory(
        commune="Dunkerque",
        code_insee="59183",
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=schema.ZoneDiffusionType.EPCI.value,
        zone_diffusion_code="245900428",
        zone_diffusion_nom="CU de Dunkerque",
    )
    factories.ServiceFactory(
        commune="Lille",
        code_insee="59350",
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=schema.ZoneDiffusionType.EPCI.value,
        zone_diffusion_code="200093201",
        zone_diffusion_nom="Métropole Européenne de Lille",
    )

    url = "/api/v0/search/services"
    response = api_client.get(
        url,
        params={
            "code_insee": "59183",  # Dunkerque
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.with_token
def test_search_services_with_zone_diffusion_departement(
    api_client, generate_communes_nord
):
    service_1 = factories.ServiceFactory(
        commune="Dunkerque",
        code_insee="59183",
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=schema.ZoneDiffusionType.DEPARTEMENT.value,
        zone_diffusion_code="59",
        zone_diffusion_nom="Nord",
    )
    factories.ServiceFactory(
        commune="Lille",
        code_insee="59350",
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=schema.ZoneDiffusionType.DEPARTEMENT.value,
        zone_diffusion_code="62",
        zone_diffusion_nom="Pas-de-Calais",
    )

    url = "/api/v0/search/services"
    response = api_client.get(
        url,
        params={
            "code_insee": "59183",  # Dunkerque
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.with_token
def test_search_services_with_zone_diffusion_region(api_client, generate_communes_nord):
    service_1 = factories.ServiceFactory(
        commune="Dunkerque",
        code_insee="59183",
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=schema.ZoneDiffusionType.REGION.value,
        zone_diffusion_code="32",
        zone_diffusion_nom="Nord",
    )
    factories.ServiceFactory(
        commune="Maubeuge",
        code_insee="59392",
        latitude=50.277500,
        longitude=3.973400,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=schema.ZoneDiffusionType.REGION.value,
        zone_diffusion_code="44",
        zone_diffusion_nom="Grand Est",
    )

    url = "/api/v0/search/services"
    response = api_client.get(
        url,
        params={
            "code_insee": "59183",  # Dunkerque
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.with_token
def test_search_services_with_bad_code_insee(
    api_client,
    generate_communes_nord,
):
    factories.ServiceFactory(
        commune="Lille",
        code_insee="59350",
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[schema.ModeAccueil.A_DISTANCE.value],
    )

    url = "/api/v0/search/services"
    response = api_client.get(
        url,
        params={
            "code_insee": "59999",  # Does not exist
        },
    )

    assert response.status_code == 422


@pytest.mark.with_token
def test_search_services_with_code_insee_ordering(
    api_client,
    generate_communes_nord,
):
    service_1 = factories.ServiceFactory(
        commune="Hazebrouck",
        code_insee="59295",
        latitude=50.7262,
        longitude=2.5387,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
    )
    service_2 = factories.ServiceFactory(
        commune="Lille",
        code_insee="59350",
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
    )
    factories.ServiceFactory(
        code_insee=None,
        _di_geocodage_code_insee=None,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
    )

    url = "/api/v0/search/services"
    response = api_client.get(url, params={"code_insee": "59009"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert resp_data["items"][0]["service"]["id"] == service_2.id
    assert 0 < resp_data["items"][0]["distance"] < 50
    assert resp_data["items"][1]["service"]["id"] == service_1.id
    assert resp_data["items"][1]["distance"] < 50


@pytest.mark.with_token
def test_search_services_with_code_insee_sample_distance(api_client):
    service_1 = factories.ServiceFactory(
        commune="Lille",
        code_insee="59350",
        _di_geocodage_code_insee=None,
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
    )
    factories.ServiceFactory(
        code_insee=None,
        _di_geocodage_code_insee=None,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
    )

    url = "/api/v0/search/services"
    response = api_client.get(url, params={"code_insee": "59295"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id
    assert resp_data["items"][0]["distance"] == 35


@pytest.mark.with_token
def test_search_services_with_code_insee_a_distance(api_client):
    service_1 = factories.ServiceFactory(
        commune="Dunkerque",
        code_insee="59183",
        _di_geocodage_code_insee=None,
        modes_accueil=[schema.ModeAccueil.A_DISTANCE.value],
    )
    service_2 = factories.ServiceFactory(
        commune="Maubeuge",
        code_insee="59392",
        _di_geocodage_code_insee=None,
        modes_accueil=[schema.ModeAccueil.A_DISTANCE.value],
    )

    url = "/api/v0/search/services"
    response = api_client.get(url, params={"code_insee": "59183"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert resp_data["items"][0]["service"]["id"] == service_1.id
    assert resp_data["items"][0]["distance"] is None
    assert resp_data["items"][1]["service"]["id"] == service_2.id
    assert resp_data["items"][1]["distance"] is None


@pytest.mark.parametrize(
    "thematiques,input,found",
    [
        ([], [schema.Thematique.FAMILLE.value], False),
        ([schema.Thematique.FAMILLE.value], [schema.Thematique.FAMILLE.value], True),
        ([schema.Thematique.NUMERIQUE.value], [schema.Thematique.FAMILLE.value], False),
        (
            [schema.Thematique.NUMERIQUE.value, schema.Thematique.FAMILLE.value],
            [schema.Thematique.FAMILLE.value],
            True,
        ),
        (
            [schema.Thematique.SANTE.value, schema.Thematique.NUMERIQUE.value],
            [schema.Thematique.FAMILLE.value, schema.Thematique.NUMERIQUE.value],
            True,
        ),
        (
            [schema.Thematique.SANTE.value, schema.Thematique.NUMERIQUE.value],
            [schema.Thematique.FAMILLE.value, schema.Thematique.NUMERIQUE.value],
            True,
        ),
        (
            [schema.Thematique.FAMILLE__GARDE_DENFANTS.value],
            [schema.Thematique.FAMILLE.value],
            True,
        ),
    ],
)
@pytest.mark.with_token
def test_search_services_with_thematique(api_client, thematiques, input, found):
    service = factories.ServiceFactory(thematiques=thematiques)
    factories.ServiceFactory(thematiques=[schema.Thematique.MOBILITE.value])

    url = "/api/v0/search/services"
    response = api_client.get(url, params={"thematiques": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert resp_data["items"][0]["service"]["id"] in [service.id]
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.with_token
def test_search_services_with_frais(api_client):
    service_1 = factories.ServiceFactory(frais=[schema.Frais.GRATUIT.value])
    service_2 = factories.ServiceFactory(frais=[schema.Frais.ADHESION.value])
    factories.ServiceFactory(frais=[schema.Frais.PASS_NUMERIQUE.value])

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
def test_search_services_with_types(api_client):
    service_1 = factories.ServiceFactory(types=[schema.TypologieService.ACCUEIL.value])
    service_2 = factories.ServiceFactory(
        types=[schema.TypologieService.ACCOMPAGNEMENT.value]
    )
    factories.ServiceFactory(types=[schema.TypologieService.AIDE_FINANCIERE.value])

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
def test_search_services_with_sources(api_client):
    service_1 = factories.ServiceFactory(source="dora")
    service_2 = factories.ServiceFactory(source="emplois-de-linclusion")
    factories.ServiceFactory(source="un-jeune-une-solution")

    url = "/api/v0/search/services"
    response = api_client.get(
        url,
        params={
            "sources": ["dora", "emplois-de-linclusion"],
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
            "sources": ["foobar"],
        },
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_search_services_outdated(api_client):
    service_1 = factories.ServiceFactory(date_suspension=None)
    service_2 = factories.ServiceFactory(date_suspension=date.today())
    factories.ServiceFactory(date_suspension=date.today() - timedelta(days=1))

    url = "/api/v0/search/services"

    # exclude outdated services by default
    response = api_client.get(url)

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert {d["service"]["id"] for d in resp_data["items"]} == {
        service_1.id,
        service_2.id,
    }

    # include outdated services with query parameter
    response = api_client.get(url, params={"inclure_suspendus": True})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=3)


@pytest.mark.with_token
def test_retrieve_service(api_client):
    service_1 = factories.ServiceFactory(source="foo", id="1")
    service_2 = factories.ServiceFactory(source="bar", id="1")
    service_3 = factories.ServiceFactory(source="foo", id="2")

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
def test_retrieve_structure(api_client):
    structure_1 = factories.StructureFactory(source="foo", id="1")
    service_1 = factories.ServiceFactory(structure=structure_1)
    structure_2 = factories.StructureFactory(source="bar", id="1")
    factories.ServiceFactory(structure=structure_2)
    structure_3 = factories.StructureFactory(source="foo", id="2")

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


class FakeSoliguideClient(soliguide.SoliguideAPIClient):
    def __init__(self):
        self.retrieved_ids = []

    def retrieve_place(self, place_id: str):
        self.retrieved_ids.append(place_id)


@pytest.mark.with_token
def test_retrieve_structure_and_notify_soliguide(api_client, app):
    structure_1 = factories.StructureFactory(
        source="soliguide", id="soliguide-structure-id"
    )

    fake_soliguide_client = FakeSoliguideClient()
    app.dependency_overrides[soliguide.SoliguideAPIClient] = (
        lambda: fake_soliguide_client
    )

    url = "/api/v0/structures/"
    response = api_client.get(url + f"{structure_1.source}/{structure_1.id}")

    assert response.status_code == 200
    assert fake_soliguide_client.retrieved_ids == ["soliguide-structure-id"]


@pytest.mark.with_token
def test_retrieve_service_and_notify_soliguide(api_client, app):
    factories.StructureFactory(source="soliguide", id="soliguide-structure-id")
    service_1 = factories.ServiceFactory(
        source="soliguide",
        id="soliguide-service-id",
        structure_id="soliguide-structure-id",
    )

    fake_soliguide_client = FakeSoliguideClient()
    app.dependency_overrides[soliguide.SoliguideAPIClient] = (
        lambda: fake_soliguide_client
    )

    url = "/api/v0/services/"
    response = api_client.get(url + f"{service_1.source}/{service_1.id}")

    assert response.status_code == 200
    assert fake_soliguide_client.retrieved_ids == ["soliguide-structure-id"]


@pytest.mark.parametrize(
    ("lien_source", "depuis", "status_code"),
    [
        ("https://dora.incubateur.net/", "les-emplois", 307),
        ("https://dora.incubateur.net/", None, 422),
        (None, "les-emplois", 404),
    ],
)
def test_redirect_service(api_client, lien_source, depuis, status_code):
    service = factories.ServiceFactory(lien_source=lien_source)

    url = "/api/v0/services/"
    params = {"mtm_campaign": "LesEmplois"}
    if depuis is not None:
        params["depuis"] = depuis
    response = api_client.get(
        url + f"{service.source}/{service.id}/redirige",
        follow_redirects=False,
        params=params,
    )

    assert response.status_code == status_code

    if status_code == 307:
        assert response.headers["location"] == f"{lien_source}?mtm_campaign=LesEmplois"
