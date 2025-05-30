import json
from datetime import date, timedelta
from unittest.mock import ANY

import pytest

from data_inclusion.api.decoupage_administratif.constants import RegionEnum
from data_inclusion.api.inclusion_data import models
from data_inclusion.api.utils import soliguide
from data_inclusion.api.utils.schema_utils import SchemaV0, SchemaV1
from data_inclusion.schema import v0 as schema

from ... import factories

DUNKERQUE = {"code_insee": "59183", "latitude": 51.0361, "longitude": 2.3770}
HAZEBROUCK = {"code_insee": "59295", "latitude": 50.7262, "longitude": 2.5387}
LILLE = {"code_insee": "59350", "latitude": 50.633333, "longitude": 3.066667}
MAUBEUGE = {"code_insee": "59392"}
PARIS = {"code_insee": "75056", "latitude": 48.866667, "longitude": 2.333333}
PARIS_11 = {"code_insee": "75111", "latitude": 48.86010, "longitude": 2.38160}
ROUBAIX = {"code_insee": "59512"}
STRASBOURG = {"code_insee": "67482"}


def list_resources_data(resp_data):
    return [item.get("service", item) for item in resp_data["items"]]


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
def test_list_structures_unauthenticated(api_client, schema_version):
    url = f"/api/{schema_version}/structures/"
    response = api_client.get(url)

    assert response.status_code == 403


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures"])
@pytest.mark.with_token
def test_list_structures_all(api_client, db_session, url, snapshot):
    structure = models.Structure(
        _di_surrogate_id="04cfed4d-d3f7-4f9f-ad97-64bb6bbd7c96",
        accessibilite="https://acceslibre.beta.gouv.fr/app/kitchen-amount/",
        adresse="49, avenue de Pichon",
        antenne=False,
        code_insee="59350",
        code_postal="46873",
        commune="Sainte CharlotteBourg",
        complement_adresse=None,
        courriel="levyalexandre@example.org",
        date_maj="2023-01-01",
        doublons=[],
        horaires_ouverture='Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
        id="lot-kitchen-amount",
        labels_autres=["Nièvre médiation numérique"],
        labels_nationaux=[],
        latitude=-20.074628,
        lien_source="https://dora.fr/kitchen-amount",
        longitude=99.899603,
        nom="Perrin",
        presentation_detail="Or personne jambe.",
        presentation_resume="Image voie battre.",
        rna="W242194892",
        score_qualite=0.7,
        siret="76475938700658",
        site_web="https://www.le.net/",
        source="dora",
        telephone="0102030405",
        thematiques=["choisir-un-metier"],
        typologie="ACI",
    )
    db_session.add(structure)
    db_session.commit()

    response = api_client.get(url)

    assert (
        json.dumps(response.json(), indent=2, ensure_ascii=False, sort_keys=True)
        == snapshot
    )


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


@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.with_token
def test_list_structures_filter_by_typology(
    api_client,
    schema_version,
    schema: SchemaV0 | SchemaV1,
):
    structure_1 = factories.StructureFactory(
        typologie=schema.TypologieStructure.ASSO.value
    )
    factories.StructureFactory(typologie=schema.TypologieStructure.CCAS.value)

    url = f"/api/{schema_version}/structures/"
    response = api_client.get(
        url, params={"typologie": schema.TypologieStructure.ASSO.value}
    )

    resp_data = response.json()
    assert_paginated_response_data(response.json(), total=1)
    assert resp_data["items"][0]["id"] == structure_1.id

    response = api_client.get(
        url, params={"typologie": schema.TypologieStructure.MUNI.value}
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.with_token
def test_list_structures_filter_by_label(
    api_client,
    schema_version,
    schema: SchemaV0 | SchemaV1,
):
    factories.StructureFactory(
        labels_nationaux=[schema.LabelNational.FRANCE_TRAVAIL.value]
    )
    structure_2 = factories.StructureFactory(
        labels_nationaux=[
            schema.LabelNational.MOBIN.value,
            schema.LabelNational.FRANCE_SERVICE.value,
        ]
    )

    url = f"/api/{schema_version}/structures/"
    response = api_client.get(
        url, params={"label_national": schema.LabelNational.FRANCE_SERVICE.value}
    )

    resp_data = response.json()
    assert_paginated_response_data(response.json(), total=1)
    assert resp_data["items"][0]["id"] == structure_2.id

    response = api_client.get(
        url, params={"label_national": schema.LabelNational.AFPA.value}
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.with_token
def test_list_sources(
    api_client,
    schema_version,
):
    url = f"/api/{schema_version}/sources/"
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


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
def test_list_services_unauthenticated(api_client, schema_version):
    url = f"/api/{schema_version}/services/"
    response = api_client.get(url)

    assert response.status_code == 403


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services"])
@pytest.mark.with_token
def test_list_services_all(api_client, db_session, url, snapshot):
    structure = models.Structure(
        _di_surrogate_id="04cfed4d-d3f7-4f9f-ad97-64bb6bbd7c96",
        accessibilite="https://acceslibre.beta.gouv.fr/app/kitchen-amount/",
        adresse="49, avenue de Pichon",
        antenne=False,
        code_insee="59350",
        code_postal="46873",
        commune="Sainte CharlotteBourg",
        complement_adresse=None,
        courriel="levyalexandre@example.org",
        date_maj="2023-01-01",
        horaires_ouverture='Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
        id="lot-kitchen-amount",
        labels_autres=["Nièvre médiation numérique"],
        labels_nationaux=[],
        latitude=-20.074628,
        lien_source="https://dora.fr/kitchen-amount",
        longitude=99.899603,
        nom="Perrin",
        presentation_detail="Or personne jambe.",
        presentation_resume="Image voie battre.",
        rna="W242194892",
        score_qualite=0.3,
        siret="76475938700658",
        site_web="https://www.le.net/",
        source="dora",
        telephone="0102030405",
        thematiques=["choisir-un-metier"],
        typologie="ACI",
    )
    service = models.Service(
        _di_surrogate_id="eafdc798-18a6-446d-acda-7bc14ba83a39",
        _di_structure_surrogate_id="04cfed4d-d3f7-4f9f-ad97-64bb6bbd7c96",
        adresse="62, rue Eugène Rodrigues",
        code_insee="59350",
        code_postal="92950",
        commune="Sainte Gabriel",
        complement_adresse=None,
        contact_nom_prenom="Thibaut de Michaud",
        contact_public=False,
        courriel="michelgerard@example.net",
        cumulable=False,
        date_creation="2022-01-01",
        date_maj="2023-01-01",
        date_suspension="2054-01-01",
        formulaire_en_ligne=None,
        frais_autres="Camarade il.",
        frais=["gratuit"],
        id="be-water-scene-wind",
        justificatifs=[],
        latitude=-77.857573,
        lien_source="https://dora.fr/be-water-scene-wind",
        longitude=-62.54684,
        modes_accueil=["a-distance"],
        modes_orientation_accompagnateur_autres=None,
        modes_orientation_accompagnateur=["telephoner"],
        modes_orientation_beneficiaire_autres=None,
        modes_orientation_beneficiaire=["telephoner"],
        nom="Munoz",
        page_web="http://aubert.net/",
        pre_requis=[],
        presentation_detail="Épaule élever un.",
        presentation_resume="Puissant fine.",
        prise_rdv="https://teixeira.fr/",
        profils=["femmes"],
        profils_precisions="Femme en situation d'insertion",
        recurrence=None,
        score_qualite=0.5,
        source="dora",
        structure_id="much-mention",
        telephone="0102030405",
        thematiques=["choisir-un-metier"],
        types=["formation"],
        zone_diffusion_code=None,
        zone_diffusion_nom=None,
        zone_diffusion_type=None,
    )
    db_session.add(structure)
    db_session.add(service)
    db_session.commit()

    response = api_client.get(url)
    assert (
        json.dumps(response.json(), indent=2, ensure_ascii=False, sort_keys=True)
        == snapshot
    )


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
@pytest.mark.parametrize(
    "profils_precisions,input,found",
    [
        ("jeunes moins de 18 ans", "jeunes", True),
        ("jeune moins de 18 ans", "jeunes", True),
        ("jeunes et personne age", "vieux", False),
        ("jeunes et personne age", "personne OR vieux", True),
        ("jeunes et personne age", "personne jeune", True),
        ("jeunes et personne age", "jeunes -personne", False),
        ("jeunes et personne age", '"personne jeune"', False),
        ("jeunes et personne agee", "âgée", True),
    ],
)
@pytest.mark.with_token
def test_can_filter_resources_by_profils_precisions(
    api_client, profils_precisions, input, found, url
):
    resource = factories.ServiceFactory(
        profils=None, profils_precisions=profils_precisions
    )
    factories.ServiceFactory(profils=None, profils_precisions="tests")

    response = api_client.get(url, params={"recherche_public": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert list_resources_data(resp_data)[0]["id"] in [resource.id]
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.parametrize("path", ["/services", "/search/services"])
@pytest.mark.parametrize(
    "profils,input,found",
    [
        ([schema.Profil.FEMMES.value], "femme", True),
        ([schema.Profil.JEUNES_16_26.value], "jeune", True),
        ([schema.Profil.FEMMES.value], "jeune", False),
        (
            [schema.Profil.DEFICIENCE_VISUELLE.value],
            "deficience jeune difficulte",
            True,
        ),
    ],
)
@pytest.mark.with_token
def test_can_filter_resources_by_profils_precisions_with_only_profils_data(
    api_client, schema_version, schema: SchemaV0 | SchemaV1, profils, input, found, url
):
    resource = factories.ServiceFactory(profils=profils, profils_precisions="")
    factories.ServiceFactory(profils=schema.Profil.RETRAITES, profils_precisions="")

    response = api_client.get(url, params={"recherche_public": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert list_resources_data(resp_data)[0]["id"] in [resource.id]
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.parametrize(
    ("schema_version", "schema", "path", "factory"),
    [
        ("v0", SchemaV0, "/services", factories.ServiceFactory),
        ("v1", SchemaV1, "/services", factories.ServiceFactory),
        ("v0", SchemaV0, "/search/services", factories.ServiceFactory),
        ("v1", SchemaV1, "/search/services", factories.ServiceFactory),
        ("v0", SchemaV0, "/structures", factories.StructureFactory),
    ],
)
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
def test_can_filter_resources_by_thematiques(
    api_client, url, schema: SchemaV0 | SchemaV1, factory, thematiques, input, found
):
    # this checks that the fixture thematiques are in the current schema
    if any(t not in schema.Thematique for t in thematiques):
        raise ValueError("Invalid fixture param")

    resource = factory(thematiques=thematiques)
    factory(thematiques=[schema.Thematique.MOBILITE.value])

    response = api_client.get(url, params={"thematiques": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert list_resources_data(resp_data)[0]["id"] in [resource.id]
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize(
    ("path", "factory"),
    [
        ("/structures", factories.StructureFactory),
        ("/services", factories.ServiceFactory),
    ],
)
def test_can_filter_resources_by_code_departement(api_client, url, factory):
    resource = factory(code_insee=PARIS["code_insee"])
    factory(code_insee=LILLE["code_insee"])
    factory(code_insee=None)

    response = api_client.get(url, params={"code_departement": "75"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == resource.id

    response = api_client.get(url, params={"code_departement": "62"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize(
    ("path", "factory"),
    [
        ("/structures", factories.StructureFactory),
        ("/services", factories.ServiceFactory),
    ],
)
def test_can_filter_resources_by_slug_departement(api_client, url, factory):
    resource = factory(code_insee=PARIS["code_insee"])
    factory(code_insee=LILLE["code_insee"])

    response = api_client.get(url, params={"slug_departement": "paris"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == resource.id

    response = api_client.get(url, params={"slug_departement": "pas-de-calais"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize(
    ("path", "factory"),
    [
        ("/structures", factories.StructureFactory),
        ("/services", factories.ServiceFactory),
    ],
)
def test_can_filter_resources_by_code_region(api_client, url, factory):
    resource = factory(code_insee=PARIS["code_insee"])
    factory(code_insee=LILLE["code_insee"])

    response = api_client.get(
        url, params={"code_region": RegionEnum.ILE_DE_FRANCE.value.code}
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == resource.id

    response = api_client.get(
        url, params={"code_region": RegionEnum.LA_REUNION.value.code}
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize(
    ("path", "factory"),
    [
        ("/structures", factories.StructureFactory),
        ("/services", factories.ServiceFactory),
    ],
)
def test_can_filter_resources_by_slug_region(api_client, url, factory):
    resource = factory(code_insee=PARIS["code_insee"])
    factory(code_insee=LILLE["code_insee"])

    response = api_client.get(
        url, params={"slug_region": RegionEnum.ILE_DE_FRANCE.value.slug}
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == resource.id

    response = api_client.get(
        url, params={"slug_region": RegionEnum.LA_REUNION.value.slug}
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize(
    ("path", "factory"),
    [
        ("/structures", factories.StructureFactory),
        ("/services", factories.ServiceFactory),
    ],
)
@pytest.mark.parametrize(
    "code_commune, input, found",
    [
        (None, DUNKERQUE["code_insee"], False),
        (DUNKERQUE["code_insee"], DUNKERQUE["code_insee"], True),
        (DUNKERQUE["code_insee"], "62041", False),
        (PARIS["code_insee"], "75056", True),
        (PARIS_11["code_insee"], "75111", True),
    ],
)
def test_can_filter_resources_by_code_commune(
    api_client, url, factory, code_commune, input, found
):
    resource = factory(code_insee=code_commune)
    factory(code_insee=LILLE["code_insee"])

    response = api_client.get(url, params={"code_commune": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert resp_data["items"][0]["id"] == resource.id
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.with_token
@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.parametrize("path", ["/services", "/search/services"])
def test_can_filter_services_by_profils(api_client, url, schema: SchemaV0 | SchemaV1):
    service_1 = factories.ServiceFactory(profils=[schema.Profil.FEMMES.value])
    service_2 = factories.ServiceFactory(profils=[schema.Profil.JEUNES_16_26.value])
    factories.ServiceFactory(profils=[schema.Profil.ADULTES.value])
    factories.ServiceFactory(profils=[])
    factories.ServiceFactory(profils=None)

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
@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.parametrize("path", ["/services", "/search/services"])
def test_list_services_by_types(api_client, schema: SchemaV0 | SchemaV1, url):
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
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
def test_list_services_by_score_qualite(api_client, url):
    service_1 = factories.ServiceFactory(score_qualite=0.5)
    service_2 = factories.ServiceFactory(score_qualite=0.7)
    factories.ServiceFactory(score_qualite=0.2)

    response = api_client.get(
        url,
        params={"score_qualite_minimum": service_1.score_qualite},
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert {d["id"] for d in list_resources_data(resp_data)} == {
        service_1.id,
        service_2.id,
    }


@pytest.mark.with_token
@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.parametrize("path", ["/services", "/search/services"])
def test_can_filter_services_by_frais(api_client, schema: SchemaV0 | SchemaV1, url):
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
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
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
@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.parametrize("path", ["/services", "/search/services"])
def test_can_filter_services_by_modes_accueil(
    api_client, url, schema: SchemaV0 | SchemaV1
):
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
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize(
    ("path", "factory"),
    [
        ("/services", factories.ServiceFactory),
        ("/search/services", factories.ServiceFactory),
        ("/structures", factories.StructureFactory),
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


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize(
    ("is_dora",),
    [
        pytest.param(True, marks=pytest.mark.with_token("dora-test")),
        pytest.param(False, marks=pytest.mark.with_token("not-dora")),
    ],
)
@pytest.mark.parametrize(
    ("path", "factory"),
    [
        ("/services", factories.ServiceFactory),
        ("/services", factories.ServiceFactory),
        ("/structures", factories.StructureFactory),
        ("/structures", factories.StructureFactory),
        ("/search/services", factories.ServiceFactory),
        ("/search/services", factories.ServiceFactory),
    ],
)
@pytest.mark.parametrize(
    ("source", "code_insee", "restricted"),
    [
        ("soliguide", PARIS["code_insee"], True),
        ("soliguide", None, True),
        ("soliguide", LILLE["code_insee"], False),
        ("soliguide", STRASBOURG["code_insee"], False),
        ("emplois-de-linclusion", PARIS["code_insee"], False),
        ("emplois-de-linclusion", None, False),
    ],
)
def test_soliguide_is_partially_available(
    api_client, is_dora, url, factory, source, code_insee, restricted
):
    factory(source=source, code_insee=code_insee)

    response = api_client.get(url)

    assert response.status_code == 200
    resp_data = response.json()

    assert (restricted and not is_dora) == (len(resp_data["items"]) == 0)


@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.parametrize(
    "commune_data, input, found",
    [
        (None, DUNKERQUE["code_insee"], False),
        (DUNKERQUE, DUNKERQUE["code_insee"], True),
        (DUNKERQUE, MAUBEUGE["code_insee"], False),
        (PARIS, "75056", True),
        (PARIS_11, "75111", True),
    ],
)
@pytest.mark.with_token
def test_search_services_with_code_commune(
    api_client, commune_data, input, found, schema: SchemaV0 | SchemaV1, url
):
    service = factories.ServiceFactory(
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
        **(commune_data if commune_data is not None else {}),
    )

    response = api_client.get(url, params={"code_commune": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert resp_data["items"][0]["service"]["id"] == service.id
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_code_commune_too_far(
    api_client, schema: SchemaV0 | SchemaV1, url
):
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


@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_pays(
    api_client, url, schema: SchemaV0 | SchemaV1
):
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


@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_commune(
    api_client, url, schema: SchemaV0 | SchemaV1
):
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


@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_epci(
    api_client, url, schema: SchemaV0 | SchemaV1
):
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


@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_departement(
    api_client, url, schema: SchemaV0 | SchemaV1
):
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


@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_region(
    api_client, url, schema: SchemaV0 | SchemaV1
):
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


@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_bad_code_commune(
    api_client, url, schema: SchemaV0 | SchemaV1
):
    factories.ServiceFactory(
        commune="Lille",
        code_insee=LILLE["code_insee"],
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[schema.ModeAccueil.A_DISTANCE.value],
    )

    response = api_client.get(
        url,
        params={
            "code_commune": "59999",  # Does not exist
        },
    )

    assert response.status_code == 422


@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_code_commune_ordering(
    api_client, url, schema: SchemaV0 | SchemaV1
):
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
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
    )

    response = api_client.get(url, params={"code_commune": ROUBAIX["code_insee"]})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert resp_data["items"][0]["service"]["id"] == service_2.id
    assert 0 < resp_data["items"][0]["distance"] < 50
    assert resp_data["items"][1]["service"]["id"] == service_1.id
    assert resp_data["items"][1]["distance"] < 50


@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_code_commune_sample_distance(
    api_client, url, schema: SchemaV0 | SchemaV1
):
    service_1 = factories.ServiceFactory(
        commune="Lille",
        **LILLE,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
    )
    factories.ServiceFactory(
        code_insee=None,
        modes_accueil=[schema.ModeAccueil.EN_PRESENTIEL.value],
    )

    response = api_client.get(url, params={"code_commune": HAZEBROUCK["code_insee"]})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id
    assert resp_data["items"][0]["distance"] == 39


@pytest.mark.parametrize(
    ("schema_version", "schema"),
    [("v0", SchemaV0), ("v1", SchemaV1)],
)
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_code_commune_a_distance(
    api_client, url, schema: SchemaV0 | SchemaV1
):
    service_1 = factories.ServiceFactory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        modes_accueil=[schema.ModeAccueil.A_DISTANCE.value],
    )
    service_2 = factories.ServiceFactory(
        commune="Maubeuge",
        code_insee=MAUBEUGE["code_insee"],
        modes_accueil=[schema.ModeAccueil.A_DISTANCE.value],
    )

    response = api_client.get(url, params={"code_commune": DUNKERQUE["code_insee"]})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert sorted([d["service"]["id"] for d in resp_data["items"]]) == sorted(
        [
            service_1.id,
            service_2.id,
        ]
    )
    assert resp_data["items"][0]["distance"] is None
    assert resp_data["items"][1]["distance"] is None


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services"])
@pytest.mark.with_token
def test_retrieve_service(api_client, url):
    service_1 = factories.ServiceFactory(source="foo", id="1")
    service_2 = factories.ServiceFactory(source="bar", id="1")
    service_3 = factories.ServiceFactory(source="foo", id="2")

    response = api_client.get(url + f"/{service_1.source}/{service_1.id}")

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data["id"] == service_1.id
    assert "structure" in resp_data
    assert resp_data["structure"]["id"] == service_1.structure.id

    response = api_client.get(url + f"{service_2.source}/{service_3.id}")
    assert response.status_code == 404


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures"])
@pytest.mark.with_token
def test_retrieve_structure(api_client, url):
    structure_1 = factories.StructureFactory(source="foo", id="1")
    service_1 = factories.ServiceFactory(structure=structure_1)
    structure_2 = factories.StructureFactory(source="bar", id="1")
    factories.ServiceFactory(structure=structure_2)
    structure_3 = factories.StructureFactory(source="foo", id="2")

    response = api_client.get(url + f"/{structure_1.source}/{structure_1.id}")

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


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures"])
@pytest.mark.with_token
def test_retrieve_structure_and_notify_soliguide(api_client, app, url):
    structure_1 = factories.StructureFactory(
        source="soliguide", id="soliguide-structure-id"
    )

    fake_soliguide_client = FakeSoliguideClient()
    app.dependency_overrides[soliguide.SoliguideAPIClient] = (
        lambda: fake_soliguide_client
    )

    response = api_client.get(url + f"/{structure_1.source}/{structure_1.id}")

    assert response.status_code == 200
    assert fake_soliguide_client.retrieved_ids == ["soliguide-structure-id"]


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services"])
@pytest.mark.parametrize(
    ("requested_id", "status_code", "retrieved_ids"),
    [
        ("soliguide-service-id", 200, ["soliguide-structure-id"]),
        ("not-a-soliguide-service-id", 404, []),
    ],
)
@pytest.mark.with_token
def test_retrieve_service_and_notify_soliguide(
    api_client, app, url, requested_id, status_code, retrieved_ids
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

    response = api_client.get(url + f"/{service_1.source}/{requested_id}")

    assert response.status_code == status_code
    assert fake_soliguide_client.retrieved_ids == retrieved_ids


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures"])
@pytest.mark.parametrize(
    ("exclure_doublons", "expected_ids"),
    [(False, {"first", "second", "third", "fourth"}), (True, {"second", "fourth"})],
)
@pytest.mark.with_token
def test_list_structures_deduplicate_flag(
    api_client, url, exclure_doublons, expected_ids
):
    factories.StructureFactory(
        source="src_1",
        id="first",
        cluster_best_duplicate="src_2-second",
        score_qualite=0.5,
    )
    factories.StructureFactory(
        source="src_2",
        id="second",
        cluster_best_duplicate="src_2-second",
        score_qualite=0.9,
    )
    factories.StructureFactory(
        source="src_2",
        id="third",
        cluster_best_duplicate="src_2-second",
        score_qualite=0.3,
    )
    factories.StructureFactory(
        source="src_3",
        id="fourth",
        score_qualite=0.8,
    )

    response = api_client.get(url, params={"exclure_doublons": exclure_doublons})

    assert_paginated_response_data(response.json(), total=len(expected_ids))
    assert {d["id"] for d in response.json()["items"]} == expected_ids


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures"])
@pytest.mark.with_token
def test_list_structures_deduplicate_flag_equal(api_client, url):
    # all these structiures have no services so we assume they have a score of 0
    factories.StructureFactory(
        date_maj="2020-01-01",
        source="src_1",
        id="first",
        cluster_best_duplicate="src_2-second",
    )
    factories.StructureFactory(
        date_maj="2024-01-01",  # the latest update
        source="src_2",
        id="second",
        cluster_best_duplicate="src_2-second",
    )
    factories.StructureFactory(
        date_maj="2008-01-01",
        source="src_3",
        id="third",
        cluster_best_duplicate="src_2-second",
    )
    factories.StructureFactory(
        source="src_3",
        id="fourth",
        cluster_best_duplicate=None,
    )

    response = api_client.get(url, params={"exclure_doublons": True})

    assert_paginated_response_data(response.json(), total=2)
    assert {d["id"] for d in response.json()["items"]} == {"second", "fourth"}


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_deduplicate_flag(api_client, url):
    s1 = factories.StructureFactory(
        date_maj="2020-01-01",
        source="src_1",
        id="first",
        cluster_best_duplicate="src_2-second",
    )
    s2 = factories.StructureFactory(
        date_maj="2024-01-01",  # the latest update
        source="src_2",
        id="second",
        cluster_best_duplicate="src_2-second",
    )
    s3 = factories.StructureFactory(
        date_maj="2008-01-01",
        source="src_3",
        id="third",
        cluster_best_duplicate="src_2-second",
    )
    s4 = factories.StructureFactory(
        source="src_3",
        id="fourth",
        cluster_best_duplicate=None,
    )

    factories.ServiceFactory(
        source="src_1",
        id="first",
        structure=s1,
    )
    factories.ServiceFactory(
        source="src_2",
        id="second",
        structure=s2,
    )
    factories.ServiceFactory(
        source="src_2",
        id="third",
        structure=s3,
    )
    factories.ServiceFactory(
        source="src_3",
        id="fourth",
        structure=s4,
    )

    response = api_client.get(url, params={"exclure_doublons": True})

    assert_paginated_response_data(response.json(), total=2)
    assert {d["service"]["id"] for d in response.json()["items"]} == {
        "second",
        "fourth",
    }
