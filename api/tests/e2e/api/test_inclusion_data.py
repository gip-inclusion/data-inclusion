import json
from datetime import date, timedelta
from unittest.mock import ANY

import pytest

from data_inclusion import schema
from data_inclusion.api.code_officiel_geo.constants import RegionEnum
from data_inclusion.api.utils import soliguide

from ... import factories

DUNKERQUE = {"code_insee": "59183", "latitude": 51.0361, "longitude": 2.3770}
HAZEBROUCK = {"code_insee": "59295", "latitude": 50.7262, "longitude": 2.5387}
LILLE = {"code_insee": "59350", "latitude": 50.633333, "longitude": 3.066667}
MAUBEUGE = {"code_insee": "59392"}
PARIS = {"code_insee": "75056", "latitude": 48.866667, "longitude": 2.333333}
ROUBAIX = {"code_insee": "59512"}


def list_resources_data(resp_data):
    return [item.get("service", item) for item in resp_data["items"]]


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
                "accessibilite": "https://acceslibre.beta.gouv.fr/app/prince-point-monde/",
                "adresse": "88, boulevard Raymond Bonnet",
                "antenne": False,
                "code_insee": "59350",
                "code_postal": "80571",
                "commune": "Sainte Bernadetteboeuf",
                "complement_adresse": None,
                "courriel": "fouquetzoe@example.com",
                "date_maj": "2023-01-01",
                "horaires_ouverture": 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                "id": "prince-point-monde",
                "labels_autres": ["Nièvre médiation numérique"],
                "labels_nationaux": [],
                "latitude": -86.2307095,
                "lien_source": "https://dora.fr/prince-point-monde",
                "longitude": 114.580692,
                "nom": "Vaillant",
                "presentation_detail": "Or personne jambe.",
                "presentation_resume": "Voie battre.",
                "rna": "W948924115",
                "siret": "59382421200611",
                "site_web": "http://berger.fr/",
                "source": "dora",
                "telephone": "0102030405",
                "thematiques": ["choisir-un-metier"],
                "typologie": "ACI",
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
    assert sorted(structure.labels_nationaux) == sorted(data["labels_nationaux"])
    assert sorted(structure.labels_autres) == sorted(data["labels_autres"])
    assert sorted(structure.thematiques) == sorted(data["thematiques"])
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
@pytest.mark.feature_deprecated
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
@pytest.mark.feature_deprecated
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
@pytest.mark.feature_deprecated
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
@pytest.mark.feature_deprecated
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
                "_di_geocodage_code_insee": "59350",
                "_di_geocodage_score": 0.89,
                "adresse": "20, rue Lambert",
                "code_insee": "59350",
                "code_postal": "22846",
                "commune": "Guichard",
                "complement_adresse": None,
                "contact_nom_prenom": "Thibaut de Michaud",
                "contact_public": False,
                "courriel": "michelgerard@example.net",
                "cumulable": False,
                "date_creation": "2022-01-01",
                "date_maj": "2023-01-01",
                "date_suspension": "2054-01-01",
                "formulaire_en_ligne": None,
                "frais_autres": "Point saint source.",
                "frais": ["gratuit"],
                "id": "rassurer-vaincre",
                "justificatifs": [],
                "latitude": 80.2434875,
                "lien_source": "https://dora.fr/rassurer-vaincre",
                "longitude": 128.091981,
                "modes_accueil": ["a-distance"],
                "modes_orientation_accompagnateur_autres": None,
                "modes_orientation_accompagnateur": ["telephoner"],
                "modes_orientation_beneficiaire_autres": None,
                "modes_orientation_beneficiaire": ["telephoner"],
                "nom": "Barbe",
                "page_web": "http://www.foucher.com/",
                "pre_requis": [],
                "presentation_detail": "Noir roi fin parmi.",
                "presentation_resume": "Épaule élever un.",
                "prise_rdv": "https://www.raymond.com/",
                "profils": ["femmes"],
                "recurrence": None,
                "source": "dora",
                "structure_id": "grace-plaindre",
                "telephone": "0102030405",
                "thematiques": ["choisir-un-metier"],
                "types": ["formation"],
                "zone_diffusion_code": None,
                "zone_diffusion_nom": None,
                "zone_diffusion_type": None,
            }
        ],
        "total": 1,
        "page": 1,
        "size": ANY,
        "pages": 1,
    }


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
@pytest.mark.feature_deprecated
def test_list_services_filter_by_source(api_client):
    service_1 = factories.ServiceFactory(source="emplois-de-linclusion")
    factories.ServiceFactory(source="dora")

    url = "/api/v0/services/"
    response = api_client.get(url, params={"source": "emplois-de-linclusion"})

    assert response.status_code == 200

    resp_data = response.json()

    assert len(resp_data["items"]) == 1
    assert resp_data["items"][0]["id"] == service_1.id
    assert resp_data["items"][0]["source"] == service_1.structure.source


@pytest.mark.with_token
@pytest.mark.feature_deprecated
def test_list_services_filter_by_thematique(api_client):
    service_1 = factories.ServiceFactory(
        source="alpha",
        id="1",
        thematiques=[
            schema.Thematique.MOBILITE.value,
            schema.Thematique.NUMERIQUE.value,
        ],
    )
    service_2 = factories.ServiceFactory(
        source="alpha",
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
@pytest.mark.feature_deprecated
def test_list_services_filter_by_categorie_thematique(api_client):
    service = factories.ServiceFactory(
        source="alpha",
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
@pytest.mark.parametrize(
    ("url", "factory"),
    [
        ("/api/v0/services", factories.ServiceFactory),
        ("/api/v0/search/services", factories.ServiceFactory),
        ("/api/v0/structures", factories.StructureFactory),
    ],
)
def test_can_filter_resources_by_thematiques(
    api_client, url, factory, thematiques, input, found
):
    service = factory(thematiques=thematiques)
    factory(thematiques=[schema.Thematique.MOBILITE.value])

    response = api_client.get(url, params={"thematiques": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert list_resources_data(resp_data)[0]["id"] in [service.id]
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.with_token
@pytest.mark.parametrize(
    ("url", "factory"),
    [
        ("/api/v0/structures", factories.StructureFactory),
        ("/api/v0/services", factories.ServiceFactory),
    ],
)
def test_can_filter_resources_by_departement_code(api_client, url, factory):
    service = factory(code_insee=PARIS["code_insee"])
    factory(code_insee=LILLE["code_insee"])
    factory(code_insee=None)

    response = api_client.get(url, params={"departement": "75"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == service.id

    response = api_client.get(url, params={"departement": "62"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize(
    ("url", "factory"),
    [
        ("/api/v0/structures", factories.StructureFactory),
        ("/api/v0/services", factories.ServiceFactory),
    ],
)
def test_can_filter_resources_by_departement_slug(api_client, url, factory):
    service = factory(code_insee=PARIS["code_insee"])
    factory(code_insee=LILLE["code_insee"])

    response = api_client.get(url, params={"departement_slug": "paris"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == service.id

    response = api_client.get(url, params={"departement_slug": "pas-de-calais"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize(
    ("url", "factory"),
    [
        ("/api/v0/structures", factories.StructureFactory),
        ("/api/v0/services", factories.ServiceFactory),
    ],
)
def test_can_filter_resources_by_code_region(api_client, url, factory):
    service = factory(code_insee=PARIS["code_insee"])
    factory(code_insee=LILLE["code_insee"])

    response = api_client.get(
        url, params={"code_region": RegionEnum.ILE_DE_FRANCE.value.code}
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == service.id

    response = api_client.get(
        url, params={"code_region": RegionEnum.LA_REUNION.value.code}
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize(
    ("url", "factory"),
    [
        ("/api/v0/structures", factories.StructureFactory),
        ("/api/v0/services", factories.ServiceFactory),
    ],
)
def test_can_filter_resources_by_slug_region(api_client, url, factory):
    service = factory(code_insee=PARIS["code_insee"])
    factory(code_insee=LILLE["code_insee"])

    response = api_client.get(
        url, params={"slug_region": RegionEnum.ILE_DE_FRANCE.value.slug}
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == service.id

    response = api_client.get(
        url, params={"slug_region": RegionEnum.LA_REUNION.value.slug}
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize(
    ("url", "factory", "query_param"),
    [
        ("/api/v0/structures", factories.StructureFactory, "code_commune"),
        ("/api/v0/services", factories.ServiceFactory, "code_commune"),
        pytest.param(
            "/api/v0/services",
            factories.ServiceFactory,
            "code_insee",
            marks=pytest.mark.feature_deprecated,
        ),
    ],
)
@pytest.mark.parametrize(
    "code_commune, input, found",
    [
        (None, DUNKERQUE["code_insee"], False),
        (DUNKERQUE["code_insee"], DUNKERQUE["code_insee"], True),
        (DUNKERQUE["code_insee"], "62041", False),
        (PARIS["code_insee"], "75101", True),
    ],
)
def test_can_filter_resources_by_code_commune(
    api_client, url, factory, code_commune, input, found, query_param
):
    service = factory(code_insee=code_commune)
    factory(code_insee=LILLE["code_insee"])

    response = api_client.get(url, params={query_param: input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert resp_data["items"][0]["id"] == service.id
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("url", ["/api/v0/services", "/api/v0/search/services"])
def test_can_filter_services_by_profils(api_client, url):
    service_1 = factories.ServiceFactory(profils=[schema.Profil.FEMMES.value])
    service_2 = factories.ServiceFactory(profils=[schema.Profil.JEUNES_16_26.value])
    factories.ServiceFactory(profils=[schema.Profil.ADULTES.value])
    factories.ServiceFactory(profils=[])
    factories.ServiceFactory(profils=None)

    url = "/api/v0/services"
    response = api_client.get(
        url,
        params={
            "profils": [
                schema.Profil.FEMMES.value,
                schema.Profil.JEUNES_16_26.value,
            ],
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert {d["id"] for d in list_resources_data(resp_data)} == {
        service_1.id,
        service_2.id,
    }

    response = api_client.get(
        url,
        params={
            "profils": schema.Profil.BENEFICIAIRES_RSA.value,
        },
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("url", ["/api/v0/services", "/api/v0/search/services"])
def test_list_services_by_types(api_client, url):
    service_1 = factories.ServiceFactory(types=[schema.TypologieService.ACCUEIL.value])
    service_2 = factories.ServiceFactory(
        types=[schema.TypologieService.ACCOMPAGNEMENT.value]
    )
    factories.ServiceFactory(types=[schema.TypologieService.AIDE_FINANCIERE.value])

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
    assert {d["id"] for d in list_resources_data(resp_data)} == {
        service_1.id,
        service_2.id,
    }

    response = api_client.get(
        url,
        params={
            "types": schema.TypologieService.ATELIER.value,
        },
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("url", ["/api/v0/services", "/api/v0/search/services"])
def test_can_filter_services_by_frais(api_client, url):
    service_1 = factories.ServiceFactory(frais=[schema.Frais.GRATUIT.value])
    service_2 = factories.ServiceFactory(frais=[schema.Frais.ADHESION.value])
    factories.ServiceFactory(frais=[schema.Frais.PASS_NUMERIQUE.value])

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
    assert {d["id"] for d in list_resources_data(resp_data)} == {
        service_1.id,
        service_2.id,
    }

    response = api_client.get(
        url,
        params={
            "frais": schema.Frais.PAYANT.value,
        },
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("url", ["/api/v0/services", "/api/v0/search/services"])
def test_can_filter_services_with_an_outdated_suspension_date(api_client, url):
    service_1 = factories.ServiceFactory(date_suspension=None)
    service_2 = factories.ServiceFactory(date_suspension=date.today())
    factories.ServiceFactory(date_suspension=date.today() - timedelta(days=1))

    # exclude outdated services by default
    response = api_client.get(url)

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert {d["id"] for d in list_resources_data(resp_data)} == {
        service_1.id,
        service_2.id,
    }

    # include outdated services with query parameter
    response = api_client.get(url, params={"inclure_suspendus": True})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=3)


@pytest.mark.with_token
@pytest.mark.parametrize("url", ["/api/v0/services", "/api/v0/search/services"])
def test_can_filter_services_by_modes_accueil(api_client, url):
    service_1 = factories.ServiceFactory(
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value]
    )
    factories.ServiceFactory(modes_accueil=[schema.ModeAccueil.A_DISTANCE.value])
    factories.ServiceFactory(modes_accueil=[])
    factories.ServiceFactory(modes_accueil=None)

    response = api_client.get(
        url,
        params={
            "modes_accueil": [
                schema.ModeAccueil.EN_PRESENTIEL.value,
            ],
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)

    assert list_resources_data(resp_data)[0]["id"] == service_1.id


@pytest.mark.with_token
@pytest.mark.parametrize(
    ("url", "factory"),
    [
        ("/api/v0/services", factories.ServiceFactory),
        ("/api/v0/search/services", factories.ServiceFactory),
        ("/api/v0/structures", factories.StructureFactory),
    ],
)
def test_can_filter_resources_by_sources(api_client, url, factory):
    service_1 = factory(source="dora")
    service_2 = factory(source="emplois-de-linclusion")
    factory(source="un-jeune-une-solution")

    response = api_client.get(
        url,
        params={
            "sources": ["dora", "emplois-de-linclusion"],
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert {d["id"] for d in list_resources_data(resp_data)} == {
        service_1.id,
        service_2.id,
    }

    response = api_client.get(
        url,
        params={
            "sources": ["foobar"],
        },
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.parametrize(
    "commune_data, input, found",
    [
        (None, DUNKERQUE["code_insee"], False),
        (DUNKERQUE, DUNKERQUE["code_insee"], True),
        (DUNKERQUE, MAUBEUGE["code_insee"], False),
        (PARIS, "75101", True),
        pytest.param(PARIS, PARIS["code_insee"], True, marks=pytest.mark.xfail),
    ],
)
@pytest.mark.with_token
@pytest.mark.parametrize(
    "query_param",
    [
        "code_commune",
        pytest.param("code_insee", marks=pytest.mark.feature_deprecated),
    ],
)
def test_search_services_with_code_commune(
    api_client, commune_data, input, found, query_param
):
    service = factories.ServiceFactory(
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
        **(commune_data if commune_data is not None else {}),
    )

    url = "/api/v0/search/services"
    response = api_client.get(url, params={query_param: input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert resp_data["items"][0]["service"]["id"] == service.id
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.with_token
def test_search_services_with_code_commune_too_far(api_client):
    # Dunkerque to Hazebrouck: <50km
    # Hazebrouck to Lille: <50km
    # Dunkerque to Lille: >50km
    service_1 = factories.ServiceFactory(
        commune="Lille",
        **LILLE,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
    )
    service_2 = factories.ServiceFactory(
        commune="Dunkerque",
        **DUNKERQUE,
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
            "code_commune": ROUBAIX["code_insee"],  # Roubaix (only close to Lille)
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
            "code_commune": ROUBAIX["code_insee"],  # Roubaix
            # Coordinates for Le Mans. We don't enforce lat/lon to be within
            # the supplied 'code_commune' city limits.
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
            "code_commune": ROUBAIX["code_insee"],  # Roubaix
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

    # What about a request without code_commune but with lat/lon?
    response = api_client.get(
        url,
        params={
            # Coordinates for Le Mans, should be ignored.
            # the supplied 'code_commune' city limits.
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
            "code_commune": MAUBEUGE["code_insee"],
            "lat": 48.003954,
        },
    )

    assert response.status_code == 422
    resp_data = response.json()
    assert resp_data["detail"] == "The `lat` and `lon` must be simultaneously filled."

    response = api_client.get(
        url,
        params={
            "code_commune": MAUBEUGE["code_insee"],
            "lon": 1.2563,
        },
    )

    assert response.status_code == 422
    resp_data = response.json()
    assert resp_data["detail"] == "The `lat` and `lon` must be simultaneously filled."


@pytest.mark.with_token
def test_search_services_with_zone_diffusion_pays(api_client):
    service_1 = factories.ServiceFactory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
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
            "code_commune": MAUBEUGE["code_insee"],  # Maubeuge
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.with_token
def test_search_services_with_zone_diffusion_commune(api_client):
    service_1 = factories.ServiceFactory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=schema.ZoneDiffusionType.COMMUNE.value,
        zone_diffusion_code=DUNKERQUE["code_insee"],
        zone_diffusion_nom="Dunkerque",
    )
    factories.ServiceFactory(
        commune="Lille",
        code_insee=LILLE["code_insee"],
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=schema.ZoneDiffusionType.COMMUNE.value,
        zone_diffusion_code=LILLE["code_insee"],
        zone_diffusion_nom="Lille",
    )

    url = "/api/v0/search/services"
    response = api_client.get(
        url,
        params={
            "code_commune": DUNKERQUE["code_insee"],  # Dunkerque
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.with_token
def test_search_services_with_zone_diffusion_epci(api_client):
    service_1 = factories.ServiceFactory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=schema.ZoneDiffusionType.EPCI.value,
        zone_diffusion_code="245900428",
        zone_diffusion_nom="CU de Dunkerque",
    )
    factories.ServiceFactory(
        commune="Lille",
        code_insee=LILLE["code_insee"],
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
            "code_commune": DUNKERQUE["code_insee"],  # Dunkerque
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.with_token
def test_search_services_with_zone_diffusion_departement(api_client):
    service_1 = factories.ServiceFactory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=schema.ZoneDiffusionType.DEPARTEMENT.value,
        zone_diffusion_code="59",
        zone_diffusion_nom="Nord",
    )
    factories.ServiceFactory(
        commune="Lille",
        code_insee=LILLE["code_insee"],
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
            "code_commune": DUNKERQUE["code_insee"],  # Dunkerque
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.with_token
def test_search_services_with_zone_diffusion_region(api_client):
    service_1 = factories.ServiceFactory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=schema.ZoneDiffusionType.REGION.value,
        zone_diffusion_code="32",
        zone_diffusion_nom="Nord",
    )
    factories.ServiceFactory(
        commune="Maubeuge",
        code_insee=MAUBEUGE["code_insee"],
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
            "code_commune": DUNKERQUE["code_insee"],  # Dunkerque
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.with_token
def test_search_services_with_bad_code_commune(api_client):
    factories.ServiceFactory(
        commune="Lille",
        code_insee=LILLE["code_insee"],
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[schema.ModeAccueil.A_DISTANCE.value],
    )

    url = "/api/v0/search/services"
    response = api_client.get(
        url,
        params={
            "code_commune": "59999",  # Does not exist
        },
    )

    assert response.status_code == 422


@pytest.mark.with_token
def test_search_services_with_code_commune_ordering(api_client):
    service_1 = factories.ServiceFactory(
        commune="Hazebrouck",
        **HAZEBROUCK,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
    )
    service_2 = factories.ServiceFactory(
        commune="Lille",
        **LILLE,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
    )
    factories.ServiceFactory(
        code_insee=None,
        _di_geocodage_code_insee=None,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
    )

    url = "/api/v0/search/services"
    response = api_client.get(url, params={"code_commune": ROUBAIX["code_insee"]})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert resp_data["items"][0]["service"]["id"] == service_2.id
    assert 0 < resp_data["items"][0]["distance"] < 50
    assert resp_data["items"][1]["service"]["id"] == service_1.id
    assert resp_data["items"][1]["distance"] < 50


@pytest.mark.with_token
def test_search_services_with_code_commune_sample_distance(api_client):
    service_1 = factories.ServiceFactory(
        commune="Lille",
        _di_geocodage_code_insee=None,
        **LILLE,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
    )
    factories.ServiceFactory(
        code_insee=None,
        _di_geocodage_code_insee=None,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
    )

    url = "/api/v0/search/services"
    response = api_client.get(url, params={"code_commune": HAZEBROUCK["code_insee"]})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id
    assert resp_data["items"][0]["distance"] == 35


@pytest.mark.with_token
def test_search_services_with_code_commune_a_distance(api_client):
    service_1 = factories.ServiceFactory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        _di_geocodage_code_insee=None,
        modes_accueil=[schema.ModeAccueil.A_DISTANCE.value],
    )
    service_2 = factories.ServiceFactory(
        commune="Maubeuge",
        code_insee=MAUBEUGE["code_insee"],
        _di_geocodage_code_insee=None,
        modes_accueil=[schema.ModeAccueil.A_DISTANCE.value],
    )

    url = "/api/v0/search/services"
    response = api_client.get(url, params={"code_commune": DUNKERQUE["code_insee"]})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert resp_data["items"][0]["service"]["id"] == service_1.id
    assert resp_data["items"][0]["distance"] is None
    assert resp_data["items"][1]["service"]["id"] == service_2.id
    assert resp_data["items"][1]["distance"] is None


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


@pytest.mark.parametrize(
    ("requested_id", "status_code", "retrieved_ids"),
    [
        ("soliguide-service-id", 200, ["soliguide-structure-id"]),
        ("not-a-soliguide-service-id", 404, []),
    ],
)
@pytest.mark.with_token
def test_retrieve_service_and_notify_soliguide(
    api_client, app, requested_id, status_code, retrieved_ids
):
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
    response = api_client.get(url + f"{service_1.source}/{requested_id}")

    assert response.status_code == status_code
    assert fake_soliguide_client.retrieved_ids == retrieved_ids
