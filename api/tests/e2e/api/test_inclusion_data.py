import json
from unittest.mock import ANY

import pytest
from furl import furl

from data_inclusion.api.decoupage_administratif import constants
from data_inclusion.api.decoupage_administratif.constants import RegionEnum
from data_inclusion.api.decoupage_administratif.models import Commune
from data_inclusion.api.inclusion_data import models
from data_inclusion.api.utils import soliguide
from data_inclusion.schema import v1

from ... import factories

DUNKERQUE = {"code_insee": "59183", "latitude": 51.0361, "longitude": 2.3770}
HAZEBROUCK = {"code_insee": "59295", "latitude": 50.7262, "longitude": 2.5387}
LILLE = {"code_insee": "59350", "latitude": 50.633333, "longitude": 3.066667}
MAUBEUGE = {"code_insee": "59392"}
PARIS = {"code_insee": "75056", "latitude": 48.866667, "longitude": 2.333333}
PARIS_11 = {"code_insee": "75111", "latitude": 48.86010, "longitude": 2.38160}
ROUBAIX = {"code_insee": "59512"}
STRASBOURG = {"code_insee": "67482"}

STRUCTURES_ENDPOINT = furl("/api/v1/structures")
SERVICES_ENDPOINT = furl("/api/v1/services")
SEARCH_ENDPOINT = furl("/api/v1/search/services")
SOURCES_ENDPOINT = furl("/api/v1/sources")


def list_resources_data(resp_data):
    return [item.get("service", item) for item in resp_data["items"]]


EXAMPLE_STRUCTURE = models.Structure(
    accessibilite_lieu="https://acceslibre.beta.gouv.fr/app/kitchen-amount/",
    adresse="49, avenue de Pichon",
    code_insee="59350",
    code_postal="46873",
    commune="Sainte CharlotteBourg",
    complement_adresse=None,
    courriel="levyalexandre@example.org",
    date_maj="2023-01-01",
    doublons=[],
    horaires_accueil="Mo-Fr 10:00-20:00",
    id="dora--lot-kitchen-amount",
    reseaux_porteurs=[],
    latitude=-20.074628,
    longitude=99.899603,
    nom="Perrin",
    description="""Lorem ipsum dolor sit amet, consectetur adipiscing elit,
                sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.""",
    score_qualite=0.7,
    siret="76475938700658",
    site_web="https://www.le.net/",
    source="dora",
    telephone="0102030405",
)

EXAMPLE_SERVICE = models.Service(
    adresse="62, rue Eugène Rodrigues",
    code_insee="59350",
    code_postal="92950",
    commune="Sainte Gabriel",
    complement_adresse=None,
    conditions_acces="Savoir lire et écrire",
    contact_nom_prenom="Thibaut de Michaud",
    courriel="michelgerard@example.net",
    date_maj="2023-01-01",
    frais_precisions="Camarade il.",
    frais="gratuit",
    horaires_accueil="Mo-Fr 10:00-20:00",
    id="dora--be-water-scene-wind",
    latitude=-77.857573,
    longitude=-62.54684,
    modes_accueil=["a-distance"],
    modes_mobilisation=["telephoner"],
    mobilisation_precisions=None,
    mobilisable_par=["usagers"],
    lien_mobilisation="https://teixeira.fr/",
    nom="Munoz",
    description="""Lorem ipsum dolor sit amet, consectetur adipiscing
                    elit, sed do eiusmod tempor incididunt ut labore et dolore magna
                    aliqua.""",
    publics=["femmes"],
    publics_precisions="Femme en situation d'insertion",
    score_qualite=0.5,
    source="dora",
    structure_id="dora--lot-kitchen-amount",
    telephone="0102030405",
    thematiques=["famille--garde-denfants"],
    type="formation",
    zone_eligibilite=None,
    volume_horaire_hebdomadaire=1,
    nombre_semaines=1,
)


def test_list_structures_unauthenticated(api_client):
    url = "/api/v1/structures/"
    response = api_client.get(url)

    assert response.status_code == 401


@pytest.mark.with_token
def test_list_structures_all(api_client, db_session, snapshot):
    db_session.merge(EXAMPLE_STRUCTURE)
    db_session.commit()

    response = api_client.get(STRUCTURES_ENDPOINT.url)

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


@pytest.mark.with_token
def test_list_structures_filter_by_reseaux_porteurs(api_client):
    structure_1 = factories.StructureFactory(
        reseaux_porteurs=[v1.ReseauPorteur.FRANCE_TRAVAIL.value]
    )
    factories.StructureFactory(reseaux_porteurs=[v1.ReseauPorteur.MISSION_LOCALE.value])
    factories.StructureFactory(reseaux_porteurs=[])
    factories.StructureFactory(reseaux_porteurs=None)

    response = api_client.get(
        STRUCTURES_ENDPOINT.url,
        params={"reseaux_porteurs": v1.ReseauPorteur.FRANCE_TRAVAIL.value},
    )

    resp_data = response.json()
    assert_paginated_response_data(response.json(), total=1)
    assert resp_data["items"][0]["id"] == structure_1.id

    response = api_client.get(
        STRUCTURES_ENDPOINT.url,
        params={"reseaux_porteurs": v1.ReseauPorteur.AFPA.value},
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
def test_list_sources(api_client):
    response = api_client.get(SOURCES_ENDPOINT.url)

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


def test_list_services_unauthenticated(api_client):
    response = api_client.get(SERVICES_ENDPOINT.url)

    assert response.status_code == 401


@pytest.mark.with_token
def test_list_services_all(api_client, db_session, snapshot):
    db_session.merge(EXAMPLE_STRUCTURE)
    db_session.merge(EXAMPLE_SERVICE)
    db_session.commit()

    response = api_client.get(SERVICES_ENDPOINT.url)
    assert (
        json.dumps(response.json(), indent=2, ensure_ascii=False, sort_keys=True)
        == snapshot
    )


@pytest.mark.parametrize("endpoint", [SERVICES_ENDPOINT, SEARCH_ENDPOINT])
@pytest.mark.parametrize(
    ("publics_precisions", "input", "found"),
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
def test_can_filter_resources_by_publics_precisions(
    api_client, publics_precisions, input, found, endpoint
):
    resource = factories.ServiceFactory(
        publics=None, publics_precisions=publics_precisions
    )
    factories.ServiceFactory(publics=None, publics_precisions="tests")

    response = api_client.get(endpoint.url, params={"recherche_public": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert list_resources_data(resp_data)[0]["id"] in [resource.id]
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.parametrize("endpoint", [SERVICES_ENDPOINT, SEARCH_ENDPOINT])
@pytest.mark.parametrize(
    ("profils", "input", "found"),
    [
        ([v1.Public.FEMMES.value], "femme", True),
        ([v1.Public.JEUNES.value], "jeune", True),
        ([v1.Public.FEMMES.value], "jeune", False),
        (
            [v1.Public.PERSONNES_EN_SITUATION_DE_HANDICAP.value],
            "handicap jeune difficulte",
            False,
        ),
    ],
)
@pytest.mark.with_token
def test_can_filter_resources_by_profils_precisions_with_only_profils_data(
    api_client, profils, input, found, endpoint
):
    resource = factories.ServiceFactory(publics=profils, publics_precisions="")
    factories.ServiceFactory(publics=[v1.Public.ETUDIANTS], publics_precisions="")

    response = api_client.get(endpoint.url, params={"recherche_public": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert list_resources_data(resp_data)[0]["id"] in [resource.id]
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.parametrize("endpoint", [SERVICES_ENDPOINT, SEARCH_ENDPOINT])
@pytest.mark.parametrize(
    ("thematiques", "input", "found"),
    [
        ([], [v1.Thematique.FAMILLE__GARDE_DENFANTS.value], False),
        (
            [v1.Thematique.FAMILLE__GARDE_DENFANTS.value],
            [v1.Thematique.FAMILLE__GARDE_DENFANTS.value],
            True,
        ),
        (
            [v1.Thematique.NUMERIQUE__ACQUERIR_UN_EQUIPEMENT.value],
            [v1.Thematique.FAMILLE__GARDE_DENFANTS.value],
            False,
        ),
        (
            [
                v1.Thematique.NUMERIQUE__ACQUERIR_UN_EQUIPEMENT.value,
                v1.Thematique.FAMILLE__GARDE_DENFANTS.value,
            ],
            [v1.Thematique.FAMILLE__GARDE_DENFANTS.value],
            True,
        ),
        (
            [
                v1.Thematique.SANTE__ACCES_AUX_SOINS.value,
                v1.Thematique.NUMERIQUE__ACQUERIR_UN_EQUIPEMENT.value,
            ],
            [
                v1.Thematique.FAMILLE__GARDE_DENFANTS.value,
                v1.Thematique.NUMERIQUE__ACQUERIR_UN_EQUIPEMENT.value,
            ],
            True,
        ),
        (
            [
                v1.Thematique.SANTE__ACCES_AUX_SOINS.value,
                v1.Thematique.NUMERIQUE__ACQUERIR_UN_EQUIPEMENT.value,
            ],
            [
                v1.Thematique.FAMILLE__GARDE_DENFANTS.value,
                v1.Thematique.NUMERIQUE__ACQUERIR_UN_EQUIPEMENT.value,
            ],
            True,
        ),
        (
            [v1.Thematique.FAMILLE__GARDE_DENFANTS.value],
            [v1.Categorie.FAMILLE.value],
            True,
        ),
    ],
)
@pytest.mark.with_token
def test_can_filter_resources_by_thematiques(
    api_client, endpoint, thematiques, input, found
):
    # this checks that the fixture thematiques are in the current schema
    if any(t not in v1.Thematique for t in thematiques):
        raise ValueError("Invalid fixture param")

    resource = factories.ServiceFactory(thematiques=thematiques)
    factories.ServiceFactory(
        thematiques=[v1.Thematique.MOBILITE__FINANCER_MA_MOBILITE.value]
    )

    response = api_client.get(endpoint.url, params={"thematiques": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert list_resources_data(resp_data)[0]["id"] in [resource.id]
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("endpoint", [STRUCTURES_ENDPOINT, SERVICES_ENDPOINT])
def test_can_filter_resources_by_code_departement(api_client, endpoint, factory):
    resource = factory(code_insee=PARIS["code_insee"])
    factory(code_insee=LILLE["code_insee"])
    factory(code_insee=None)

    response = api_client.get(endpoint.url, params={"code_departement": "75"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == resource.id

    response = api_client.get(endpoint.url, params={"code_departement": "62"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("endpoint", [STRUCTURES_ENDPOINT, SERVICES_ENDPOINT])
def test_can_filter_resources_by_slug_departement(api_client, endpoint, factory):
    resource = factory(code_insee=PARIS["code_insee"])
    factory(code_insee=LILLE["code_insee"])

    response = api_client.get(endpoint.url, params={"slug_departement": "paris"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == resource.id

    response = api_client.get(
        endpoint.url, params={"slug_departement": "pas-de-calais"}
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("endpoint", [STRUCTURES_ENDPOINT, SERVICES_ENDPOINT])
def test_can_filter_resources_by_code_region(api_client, endpoint, factory):
    kwargs = {}
    if "services" in endpoint.url:
        # check a structure whose location is very different from the service
        kwargs["structure__code_insee"] = STRASBOURG["code_insee"]
    resource = factory(code_insee=PARIS["code_insee"], **kwargs)
    factory(code_insee=LILLE["code_insee"])

    response = api_client.get(
        endpoint.url, params={"code_region": RegionEnum.ILE_DE_FRANCE.value.code}
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == resource.id

    response = api_client.get(
        endpoint.url, params={"code_region": RegionEnum.LA_REUNION.value.code}
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("endpoint", [STRUCTURES_ENDPOINT, SERVICES_ENDPOINT])
def test_can_filter_resources_by_slug_region(api_client, endpoint, factory):
    resource = factory(code_insee=PARIS["code_insee"])
    factory(code_insee=LILLE["code_insee"])

    response = api_client.get(
        endpoint.url, params={"slug_region": RegionEnum.ILE_DE_FRANCE.value.slug}
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == resource.id

    response = api_client.get(
        endpoint.url, params={"slug_region": RegionEnum.LA_REUNION.value.slug}
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("endpoint", [STRUCTURES_ENDPOINT, SERVICES_ENDPOINT])
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
    api_client, endpoint, factory, code_commune, input, found
):
    resource = factory(code_insee=code_commune)
    factory(code_insee=LILLE["code_insee"])

    response = api_client.get(endpoint.url, params={"code_commune": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert resp_data["items"][0]["id"] == resource.id
    else:
        assert_paginated_response_data(resp_data, total=0)


UNDEFINED = "UNDEFINED"


@pytest.mark.with_token
@pytest.mark.parametrize("endpoint", [SERVICES_ENDPOINT, SEARCH_ENDPOINT])
@pytest.mark.parametrize(
    ("defined_publics", "requested_publics", "expected_total"),
    [
        ([v1.Public.FEMMES, v1.Public.SENIORS], [v1.Public.FEMMES], 1),
        # same, but use the second defined value
        ([v1.Public.FEMMES, v1.Public.SENIORS], [v1.Public.SENIORS], 1),
        # when requested values are overlapping, but not included,
        # they should match
        (
            [v1.Public.FEMMES, v1.Public.SENIORS],
            [v1.Public.FEMMES, v1.Public.JEUNES],
            1,
        ),
        # when `tous-publics` is defined, it should match any requested values
        ([v1.Public.TOUS_PUBLICS], [v1.Public.JEUNES], 1),
        # when `tous-publics` is requested, it should match any defined values
        ([v1.Public.JEUNES], [v1.Public.TOUS_PUBLICS], 1),
        # when there isn't a requested public, it should match all
        ([v1.Public.FEMMES], UNDEFINED, 1),
        ([v1.Public.FEMMES, v1.Public.SENIORS], [v1.Public.JEUNES], 0),
        ([], [v1.Public.FEMMES], 0),
        (None, [v1.Public.FEMMES], 0),
    ],
)
def test_can_filter_services_by_publics(
    api_client,
    endpoint,
    defined_publics,
    requested_publics,
    expected_total,
):
    factories.ServiceFactory(
        publics=[p.value for p in defined_publics]
        if isinstance(defined_publics, list)
        else defined_publics
    )

    if requested_publics == UNDEFINED:
        params = {}
    elif isinstance(requested_publics, list):
        params = {"publics": [p.value for p in requested_publics]}

    response = api_client.get(endpoint.url, params=params)

    assert response.status_code == 200
    assert_paginated_response_data(response.json(), total=expected_total)


@pytest.mark.with_token
@pytest.mark.parametrize("endpoint", [SERVICES_ENDPOINT, SEARCH_ENDPOINT])
def test_list_services_by_types(api_client, endpoint):
    service_1 = factories.ServiceFactory(type=v1.TypeService.INFORMATION.value)
    service_2 = factories.ServiceFactory(type=v1.TypeService.ACCOMPAGNEMENT.value)
    factories.ServiceFactory(type=v1.TypeService.AIDE_FINANCIERE.value)

    response = api_client.get(
        endpoint.url,
        params={
            "types": [
                v1.TypeService.INFORMATION.value,
                v1.TypeService.ACCOMPAGNEMENT.value,
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
        endpoint.url,
        params={
            "types": v1.TypeService.ATELIER.value,
        },
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("endpoint", [SERVICES_ENDPOINT, SEARCH_ENDPOINT])
def test_list_services_by_score_qualite(api_client, endpoint):
    service_1 = factories.ServiceFactory(score_qualite=0.5)
    service_2 = factories.ServiceFactory(score_qualite=0.7)
    factories.ServiceFactory(score_qualite=0.2)

    response = api_client.get(
        endpoint.url,
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
@pytest.mark.parametrize("endpoint", [SERVICES_ENDPOINT, SEARCH_ENDPOINT])
def test_can_filter_services_by_frais(api_client, endpoint):
    service_1 = factories.ServiceFactory(frais=v1.Frais.GRATUIT.value)
    factories.ServiceFactory(frais=v1.Frais.PAYANT.value)

    response = api_client.get(
        endpoint.url,
        params={
            "frais": [
                v1.Frais.GRATUIT.value,
            ],
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert {d["id"] for d in list_resources_data(resp_data)} == {
        service_1.id,
    }


@pytest.mark.with_token
@pytest.mark.parametrize("endpoint", [SERVICES_ENDPOINT, SEARCH_ENDPOINT])
def test_can_filter_services_by_modes_accueil(api_client, endpoint):
    service_1 = factories.ServiceFactory(
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value]
    )
    factories.ServiceFactory(modes_accueil=[v1.ModeAccueil.A_DISTANCE.value])
    factories.ServiceFactory(modes_accueil=[])
    factories.ServiceFactory(modes_accueil=None)

    response = api_client.get(
        endpoint.url,
        params={
            "modes_accueil": [
                v1.ModeAccueil.EN_PRESENTIEL.value,
            ],
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)

    assert list_resources_data(resp_data)[0]["id"] == service_1.id


@pytest.mark.with_token
@pytest.mark.parametrize(
    "endpoint", [SERVICES_ENDPOINT, SEARCH_ENDPOINT, STRUCTURES_ENDPOINT]
)
def test_can_filter_resources_by_sources(api_client, endpoint, factory):
    service_1 = factory(source="dora")
    service_2 = factory(source="emplois-de-linclusion")
    factory(source="un-jeune-une-solution")

    response = api_client.get(
        endpoint.url,
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
        endpoint.url,
        params={
            "sources": ["foobar"],
        },
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.parametrize(
    ("is_dora",),
    [
        pytest.param(True, marks=pytest.mark.with_token("dora-test")),
        pytest.param(False, marks=pytest.mark.with_token("not-dora")),
        pytest.param(True, marks=pytest.mark.with_token("les-emplois-2025-04-10")),
    ],
)
@pytest.mark.parametrize(
    "endpoint", [SERVICES_ENDPOINT, SEARCH_ENDPOINT, STRUCTURES_ENDPOINT]
)
@pytest.mark.parametrize(
    ("source", "code_insee", "restricted"),
    [
        ("soliguide", PARIS["code_insee"], True),
        ("soliguide", None, True),
        ("emplois-de-linclusion", PARIS["code_insee"], False),
        ("emplois-de-linclusion", None, False),
        # 59 and 67 wew part of the 2025 soliguide experiment which has ended
        ("soliguide", LILLE["code_insee"], True),
        ("soliguide", STRASBOURG["code_insee"], True),
    ],
)
def test_soliguide_is_partially_available(
    api_client, is_dora, endpoint, factory, source, code_insee, restricted
):
    factory(source=source, code_insee=code_insee)

    response = api_client.get(endpoint.url)

    assert response.status_code == 200
    resp_data = response.json()

    assert (restricted and not is_dora) == (len(resp_data["items"]) == 0)


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
def test_search_services_with_code_commune(api_client, commune_data, input, found):
    service = factories.ServiceFactory(
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
        **(commune_data if commune_data is not None else {}),
    )

    response = api_client.get(SEARCH_ENDPOINT.url, params={"code_commune": input})

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
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
    )
    service_2 = factories.ServiceFactory(
        commune="Dunkerque",
        **DUNKERQUE,
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
    )
    factories.ServiceFactory(
        code_insee=None,
        latitude=None,
        longitude=None,
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
    )

    response = api_client.get(
        SEARCH_ENDPOINT.url,
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
        SEARCH_ENDPOINT.url,
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
        SEARCH_ENDPOINT.url,
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
        SEARCH_ENDPOINT.url,
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
        SEARCH_ENDPOINT.url,
        params={
            "code_commune": MAUBEUGE["code_insee"],
            "lat": 48.003954,
        },
    )

    assert response.status_code == 422
    resp_data = response.json()
    assert resp_data["detail"] == "The `lat` and `lon` must be simultaneously filled."

    response = api_client.get(
        SEARCH_ENDPOINT.url,
        params={
            "code_commune": MAUBEUGE["code_insee"],
            "lon": 1.2563,
        },
    )

    assert response.status_code == 422
    resp_data = response.json()
    assert resp_data["detail"] == "The `lat` and `lon` must be simultaneously filled."


@pytest.mark.parametrize(
    "zone_eligibilite",
    [constants.PaysEnum.FRANCE.value.code, constants.PaysEnum.FRANCE.value.slug],
)
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_pays(api_client, zone_eligibilite):
    service_1 = factories.ServiceFactory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[v1.ModeAccueil.A_DISTANCE.value],
        zone_eligibilite=[zone_eligibilite],
    )

    response = api_client.get(
        SEARCH_ENDPOINT.url,
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
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
        zone_eligibilite=[DUNKERQUE["code_insee"]],
    )

    factories.ServiceFactory(
        commune="Lille",
        code_insee=LILLE["code_insee"],
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
        zone_eligibilite=[LILLE["code_insee"]],
    )

    response = api_client.get(
        SEARCH_ENDPOINT.url,
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
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
        zone_eligibilite=["245900428"],  # CU de Dunkerque
    )
    factories.ServiceFactory(
        commune="Lille",
        code_insee=LILLE["code_insee"],
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
        zone_eligibilite=["200093201"],  # Métropole Européenne de Lille
    )

    response = api_client.get(
        SEARCH_ENDPOINT.url,
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
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
        zone_eligibilite=["59"],  # Nord
    )
    factories.ServiceFactory(
        commune="Lille",
        code_insee=LILLE["code_insee"],
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
        zone_eligibilite=["62"],  # Pas-de-Calais
    )

    response = api_client.get(
        SEARCH_ENDPOINT.url,
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
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
        zone_eligibilite=["59"],  # Nord
    )
    factories.ServiceFactory(
        commune="Maubeuge",
        code_insee=MAUBEUGE["code_insee"],
        latitude=50.277500,
        longitude=3.973400,
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
        zone_eligibilite=["44"],  # Grand Est
    )

    response = api_client.get(
        SEARCH_ENDPOINT.url,
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
        modes_accueil=[v1.ModeAccueil.A_DISTANCE.value],
    )

    response = api_client.get(
        SEARCH_ENDPOINT.url,
        params={
            "code_commune": "59999",  # Does not exist
        },
    )

    assert response.status_code == 422


@pytest.mark.with_token
def test_search_services_with_commune_without_epci(api_client, db_session):
    commune_code = "97801"
    commune_without_epci = Commune(
        code=commune_code,
        nom="Cayenne",
        departement="973",
        region="03",
        siren_epci=None,
        codes_postaux=["97300"],
        centre="POINT (-52.3333 4.9333)",
    )
    db_session.add(commune_without_epci)
    db_session.commit()

    factories.ServiceFactory(
        commune="Cayenne",
        code_insee=commune_code,
        latitude=4.9333,
        longitude=-52.3333,
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
    )

    response = api_client.get(
        SEARCH_ENDPOINT.url, params={"code_commune": commune_code}
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data["total"] >= 0


@pytest.mark.with_token
def test_search_services_with_code_commune_ordering(api_client):
    service_1 = factories.ServiceFactory(
        commune="Hazebrouck",
        **HAZEBROUCK,
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
    )
    service_2 = factories.ServiceFactory(
        commune="Lille",
        **LILLE,
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
    )
    factories.ServiceFactory(
        code_insee=None,
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
    )

    response = api_client.get(
        SEARCH_ENDPOINT.url, params={"code_commune": ROUBAIX["code_insee"]}
    )

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
        **LILLE,
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
    )
    factories.ServiceFactory(
        code_insee=None,
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
    )

    response = api_client.get(
        SEARCH_ENDPOINT.url, params={"code_commune": HAZEBROUCK["code_insee"]}
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id
    assert resp_data["items"][0]["distance"] == 39


@pytest.mark.with_token
def test_search_services_with_code_commune_a_distance(api_client):
    service_1 = factories.ServiceFactory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        modes_accueil=[v1.ModeAccueil.A_DISTANCE.value],
    )
    service_2 = factories.ServiceFactory(
        commune="Maubeuge",
        code_insee=MAUBEUGE["code_insee"],
        modes_accueil=[v1.ModeAccueil.A_DISTANCE.value],
    )

    response = api_client.get(
        SEARCH_ENDPOINT.url, params={"code_commune": DUNKERQUE["code_insee"]}
    )

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


@pytest.mark.with_token
def test_search_services_with_code_commune_mixed_modes_far_away(api_client):
    factories.ServiceFactory(
        commune="Paris",
        **PARIS,
        modes_accueil=[
            v1.ModeAccueil.EN_PRESENTIEL.value,
            v1.ModeAccueil.A_DISTANCE.value,
        ],
    )

    response = api_client.get(
        SEARCH_ENDPOINT.url, params={"code_commune": LILLE["code_insee"]}
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["distance"] is None


@pytest.mark.with_token
def test_retrieve_service(api_client):
    service_1 = factories.ServiceFactory(source="foo", id="foo--1")
    # ensure there is another service to not trivially select "the latest"
    factories.ServiceFactory(source="bar", id="bar--1")

    response = api_client.get((SERVICES_ENDPOINT / service_1.id).url)

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data["id"] == service_1.id
    assert "structure" in resp_data
    assert resp_data["structure"]["id"] == service_1.structure.id

    response = api_client.get((SERVICES_ENDPOINT / "bar--42").url)
    assert response.status_code == 404


@pytest.mark.with_token
def test_retrieve_structure(api_client):
    structure_1 = factories.StructureFactory(source="foo", id="foo--1", cluster_id="1")
    service_1 = factories.ServiceFactory(structure=structure_1)
    structure_2 = factories.StructureFactory(source="bar", id="bar--1", cluster_id="1")
    factories.ServiceFactory(structure=structure_2)

    response = api_client.get((STRUCTURES_ENDPOINT / structure_1.id).url)

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data["id"] == structure_1.id
    assert "services" in resp_data
    assert len(resp_data["services"]) == 1
    assert resp_data["services"][0]["id"] == service_1.id
    assert "doublons" in resp_data
    assert len(resp_data["doublons"]) == 1
    assert resp_data["doublons"][0]["id"] == structure_2.id

    response = api_client.get((STRUCTURES_ENDPOINT / "foo--42").url)
    assert response.status_code == 404


class FakeSoliguideClient(soliguide.SoliguideAPIClient):
    def __init__(self):
        self.retrieved_ids = []

    def retrieve_place(self, place_id: str):
        self.retrieved_ids.append(place_id)


@pytest.mark.with_token
def test_retrieve_structure_and_notify_soliguide(api_client, app):
    structure_1 = factories.StructureFactory(
        source="soliguide", id="soliguide--place-id"
    )

    fake_soliguide_client = FakeSoliguideClient()
    app.dependency_overrides[soliguide.SoliguideAPIClient] = lambda: (
        fake_soliguide_client
    )

    response = api_client.get((STRUCTURES_ENDPOINT / structure_1.id).url)

    assert response.status_code == 200
    assert fake_soliguide_client.retrieved_ids == ["place-id"]


@pytest.mark.parametrize(
    ("requested_id", "status_code", "retrieved_ids"),
    [
        ("soliguide--service-id", 200, ["place-id"]),
        ("soliguide--42", 404, []),
    ],
)
@pytest.mark.with_token
def test_retrieve_service_and_notify_soliguide(
    api_client,
    app,
    requested_id,
    status_code,
    retrieved_ids,
):
    factories.ServiceFactory(
        source="soliguide",
        id="soliguide--service-id",
        structure__source="soliguide",
        structure__id="soliguide--place-id",
    )

    fake_soliguide_client = FakeSoliguideClient()
    app.dependency_overrides[soliguide.SoliguideAPIClient] = lambda: (
        fake_soliguide_client
    )

    response = api_client.get((SERVICES_ENDPOINT / requested_id).url)

    assert response.status_code == status_code
    assert fake_soliguide_client.retrieved_ids == retrieved_ids


@pytest.mark.with_token
def test_list_structures_deduplicate(api_client):
    best_in_cluster_1_for_src_1 = factories.StructureFactory(
        source="src_1",
        id="src_1--best",
        cluster_id="cluster_1",
        score_qualite=0.5,
    )
    factories.StructureFactory(
        source="src_1",
        id="src_1--lower",
        cluster_id="cluster_1",
        score_qualite=0.3,
    )
    best_in_cluster_1_global = factories.StructureFactory(
        source="src_2",
        id="src_2--best",
        cluster_id="cluster_1",
        score_qualite=0.9,
    )
    best_in_cluster_2 = factories.StructureFactory(
        source="src_1",
        id="src_1--latest",
        cluster_id="cluster_2",
        score_qualite=0.6,
        date_maj="2024-06-01",
    )
    factories.StructureFactory(
        source="src_1",
        id="src_1--older",
        cluster_id="cluster_2",
        score_qualite=0.6,
        date_maj="2020-01-01",
    )
    no_cluster = factories.StructureFactory(
        source="src_1",
        id="src_1--no-cluster",
        score_qualite=0.8,
        cluster_id=None,
    )

    response = api_client.get(
        STRUCTURES_ENDPOINT.url,
        params={"sources": ["src_1"], "exclure_doublons": True},
    )
    assert_paginated_response_data(response.json(), total=3)
    assert {d["id"] for d in response.json()["items"]} == {
        best_in_cluster_1_for_src_1.id,
        best_in_cluster_2.id,
        no_cluster.id,
    }

    response = api_client.get(
        STRUCTURES_ENDPOINT.url,
        params={"exclure_doublons": True},
    )
    assert_paginated_response_data(response.json(), total=3)
    assert {d["id"] for d in response.json()["items"]} == {
        best_in_cluster_1_global.id,
        best_in_cluster_2.id,
        no_cluster.id,
    }


@pytest.mark.with_token
def test_search_services_deduplicate(api_client):
    best_in_cluster_1_for_src_1 = factories.StructureFactory(
        source="src_1",
        id="src_1--best",
        cluster_id="cluster_1",
        score_qualite=0.5,
    )
    lower_in_cluster_1 = factories.StructureFactory(
        source="src_1",
        id="src_1--lower",
        cluster_id="cluster_1",
        score_qualite=0.3,
    )
    best_in_cluster_1_global = factories.StructureFactory(
        source="src_2",
        id="src_2--best",
        cluster_id="cluster_1",
        score_qualite=0.9,
    )
    latest_in_cluster_2 = factories.StructureFactory(
        source="src_1",
        id="src_1--latest",
        cluster_id="cluster_2",
        score_qualite=0.6,
        date_maj="2024-06-01",
    )
    older_in_cluster_2 = factories.StructureFactory(
        source="src_1",
        id="src_1--older",
        cluster_id="cluster_2",
        score_qualite=0.6,
        date_maj="2020-01-01",
    )
    no_cluster = factories.StructureFactory(
        source="src_1",
        id="src_1--no-cluster",
        cluster_id=None,
        score_qualite=0.4,
    )

    best_structure_svc_a = factories.ServiceFactory(
        source="src_1",
        id="src_1--svc-a",
        structure=best_in_cluster_1_for_src_1,
        thematiques=["famille--garde-denfants"],
    )
    best_structure_svc_b = factories.ServiceFactory(
        source="src_1",
        id="src_1--svc-b",
        structure=best_in_cluster_1_for_src_1,
        thematiques=["mobilite--acceder-a-un-vehicule"],
    )
    factories.ServiceFactory(
        source="src_1",
        id="src_1--svc-excluded",
        structure=lower_in_cluster_1,
        thematiques=["famille--garde-denfants"],
    )
    latest_structure_svc = factories.ServiceFactory(
        source="src_1",
        id="src_1--svc-latest",
        structure=latest_in_cluster_2,
        thematiques=["famille--garde-denfants"],
    )
    factories.ServiceFactory(
        source="src_1",
        id="src_1--svc-older",
        structure=older_in_cluster_2,
        thematiques=["famille--garde-denfants"],
    )
    no_cluster_structure_svc = factories.ServiceFactory(
        source="src_1",
        id="src_1--svc-no-cluster",
        structure=no_cluster,
        thematiques=["mobilite--acceder-a-un-vehicule"],
    )
    global_best_structure_svc = factories.ServiceFactory(
        source="src_2",
        id="src_2--svc-global-best",
        structure=best_in_cluster_1_global,
        thematiques=["famille--garde-denfants"],
    )

    response = api_client.get(
        SEARCH_ENDPOINT.url,
        params={"sources": ["src_1"], "exclure_doublons": True},
    )
    assert_paginated_response_data(response.json(), total=4)
    assert {d["service"]["id"] for d in response.json()["items"]} == {
        best_structure_svc_a.id,
        best_structure_svc_b.id,
        latest_structure_svc.id,
        no_cluster_structure_svc.id,
    }

    response = api_client.get(
        SEARCH_ENDPOINT.url,
        params={"exclure_doublons": True},
    )
    assert_paginated_response_data(response.json(), total=3)
    assert {d["service"]["id"] for d in response.json()["items"]} == {
        global_best_structure_svc.id,
        latest_structure_svc.id,
        no_cluster_structure_svc.id,
    }

    response = api_client.get(
        SEARCH_ENDPOINT.url,
        params={
            "sources": ["src_1"],
            "thematiques": ["famille--garde-denfants"],
            "exclure_doublons": True,
        },
    )
    assert_paginated_response_data(response.json(), total=2)
    assert {d["service"]["id"] for d in response.json()["items"]} == {
        best_structure_svc_a.id,
        latest_structure_svc.id,
    }

    response = api_client.get(
        SEARCH_ENDPOINT.url,
        params={"thematiques": ["famille--garde-denfants"], "exclure_doublons": True},
    )
    assert_paginated_response_data(response.json(), total=2)
    assert {d["service"]["id"] for d in response.json()["items"]} == {
        global_best_structure_svc.id,
        latest_structure_svc.id,
    }

    response = api_client.get(
        SEARCH_ENDPOINT.url,
        params={
            "sources": ["src_1"],
            "thematiques": ["mobilite--acceder-a-un-vehicule"],
            "exclure_doublons": True,
        },
    )
    assert_paginated_response_data(response.json(), total=2)
    assert {d["service"]["id"] for d in response.json()["items"]} == {
        best_structure_svc_b.id,
        no_cluster_structure_svc.id,
    }


@pytest.mark.with_token
def test_search_services_deduplicate_best_structure_has_no_service(api_client):
    factories.StructureFactory(
        source="src_1",
        id="src_1--best-no-service",
        cluster_id="cluster_1",
        score_qualite=0.9,
    )
    lower_structure_with_service = factories.StructureFactory(
        source="src_1",
        id="src_1--lower-with-service",
        cluster_id="cluster_1",
        score_qualite=0.7,
    )
    service_on_lower_structure = factories.ServiceFactory(
        source="src_1",
        id="src_1--svc",
        structure=lower_structure_with_service,
        thematiques=["famille--garde-denfants"],
    )

    response = api_client.get(SEARCH_ENDPOINT.url, params={"exclure_doublons": True})

    assert response.status_code == 200
    assert {d["service"]["id"] for d in response.json()["items"]} == {
        service_on_lower_structure.id,
    }


@pytest.mark.parametrize(
    "endpoint", [SEARCH_ENDPOINT, SERVICES_ENDPOINT, STRUCTURES_ENDPOINT]
)
@pytest.mark.with_token
def test_ressources_ordered_by_id(api_client, endpoint, factory):
    # Create 10 rows with ids in **reverse** order
    for i in reversed(range(10)):
        factory_kwargs = {
            "id": f"{str(i // 2)}--{str(i)}",
            "source": str(i // 2),
        }
        if "search" in endpoint.url:
            factory_kwargs["modes_accueil"] = [v1.ModeAccueil.EN_PRESENTIEL]
        factory(**factory_kwargs)

    response = api_client.get(endpoint.url)
    assert response.status_code == 200
    items_data = response.json()["items"]
    assert [
        d["service"]["id"] if "search" in endpoint.url else d["id"] for d in items_data
    ] == [
        "0--0",
        "0--1",
        "1--2",
        "1--3",
        "2--4",
        "2--5",
        "3--6",
        "3--7",
        "4--8",
        "4--9",
    ]


@pytest.mark.with_token
@pytest.mark.parametrize(
    ("query_params", "extra_data", "expected"),
    [
        ({}, {"foo": "bar"}, None),
        ({"extra": False}, None, None),
        ({"extra": False}, {"foo": "bar"}, None),
        ({"extra": True}, None, {"extra": None}),
        ({"extra": True}, {"foo": "bar"}, {"extra": {"foo": "bar"}}),
    ],
)
def test_show_extra_data_if_flag_provided(
    api_client, query_params, extra_data, expected
):
    factories.ServiceFactory(
        id="src--1",
        source="src",
        extra=extra_data,
    )

    response = api_client.get(SERVICES_ENDPOINT.url, params=query_params)

    assert response.status_code == 200
    resp_data = response.json()
    service_data = resp_data["items"][0]

    if expected is None:
        assert "extra" not in service_data
    else:
        assert service_data["extra"] == expected["extra"]
