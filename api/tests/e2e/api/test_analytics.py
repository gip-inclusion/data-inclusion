import pytest
import sqlalchemy as sqla

from data_inclusion.api.analytics.v0 import models as v0
from data_inclusion.api.analytics.v1 import models as v1

DUNKERQUE = {"code_insee": "59183", "latitude": 51.0361, "longitude": 2.3770}
HAZEBROUCK = {"code_insee": "59295", "latitude": 50.7262, "longitude": 2.5387}


@pytest.mark.parametrize(
    ("schema_version", "path", "model"),
    [
        ("v0", "/structures/foo/bar", v0.ConsultStructureEvent),
        ("v0", "/services/foo/bar", v0.ConsultServiceEvent),
        ("v1", "/structures/foo--bar", v1.ConsultStructureEvent),
        ("v1", "/services/foo--bar", v1.ConsultServiceEvent),
    ],
)
@pytest.mark.with_token
def test_ignore_event_if_resource_not_found(api_client, db_session, url, model):
    response = api_client.get(url)

    assert response.status_code == 404
    assert db_session.scalar(sqla.select(sqla.func.count()).select_from(model)) == 0


@pytest.mark.parametrize(
    ("schema_version", "path", "model"),
    [
        ("v0", "/structures", v0.ConsultStructureEvent),
        ("v1", "/structures", v1.ConsultStructureEvent),
    ],
)
@pytest.mark.parametrize(
    ("user_agent", "num_events"),
    [
        ("bingbot/2.0; +http://www.bing.com/bingbot.htm)", 0),
        ("python-requests/2.28.1", 1),
    ],
)
@pytest.mark.with_token
def test_consult_structure_event_saved(
    api_client,
    db_session,
    url,
    user_agent,
    num_events,
    structure_factory,
    model,
):
    structure = structure_factory(source="foo", id="foo--1")

    if "v0" in url:
        url = f"{url}/{structure.source}/{structure.id}"
    else:
        url = f"{url}/{structure.id}"

    response = api_client.get(
        url,
        headers={
            "User-Agent": user_agent,
        },
    )

    assert response.status_code == 200
    assert (
        db_session.scalar(sqla.select(sqla.func.count()).select_from(model))
        == num_events
    )

    event = db_session.scalars(sqla.select(model)).first()
    if event:
        assert event.user == "some_user"
        assert event.structure_id == structure.id


@pytest.mark.parametrize(
    ("schema_version", "path", "model"),
    [
        ("v0", "/services", v0.ConsultServiceEvent),
        ("v1", "/services", v1.ConsultServiceEvent),
    ],
)
@pytest.mark.parametrize(
    ("user_agent", "num_events"),
    [
        ("bingbot/2.0; +http://www.bing.com/bingbot.htm)", 0),
        ("python-requests/2.28.1", 1),
    ],
)
@pytest.mark.with_token
def test_consult_service_event_saved(
    api_client,
    db_session,
    url,
    user_agent,
    num_events,
    service_factory,
    model,
):
    service = service_factory(source="foo", id="foo--1", score_qualite=0.8)

    if "v0" in url:
        url = f"{url}/{service.source}/{service.id}"
    else:
        url = f"{url}/{service.id}"

    response = api_client.get(
        url,
        headers={
            "User-Agent": user_agent,
        },
    )

    assert response.status_code == 200
    assert (
        db_session.scalar(sqla.select(sqla.func.count()).select_from(model))
        == num_events
    )

    event = db_session.scalars(sqla.select(model)).first()
    if event:
        assert event.user == "some_user"
        assert event.service_id == service.id
        assert event.score_qualite == service.score_qualite


@pytest.mark.parametrize(
    ("schema_version", "path", "model"),
    [
        ("v0", "/services", v0.ListServicesEvent),
        ("v1", "/services", v1.ListServicesEvent),
    ],
)
@pytest.mark.with_token
def test_list_services_event_saved(api_client, db_session, url, schema_version, model):
    query_param = {
        "sources": ["foo"],
        "thematiques": ["famille--garde-denfants"],
        "code_departement": "26",
        "code_region": "84",
        "code_commune": "26400",
        "frais": ["gratuit"],
        "modes_accueil": ["a-distance"],
        "types": ["accompagnement"],
        "recherche_public": "test",
        "score_qualite_minimum": 0.5,
    }
    if schema_version == "v0":
        query_param["profils"] = ["etudiants"]
        query_param["profils_precisions"] = "test"
    elif schema_version == "v1":
        query_param["publics"] = ["etudiants"]
        query_param["publics_precisions"] = "test"

    response = api_client.get(url, params=query_param)

    assert response.status_code == 200
    assert db_session.scalar(sqla.select(sqla.func.count()).select_from(model)) == 1

    event = db_session.scalars(sqla.select(model)).first()
    assert event.user == "some_user"
    assert event.sources == query_param["sources"]
    assert event.thematiques == query_param["thematiques"]
    assert event.code_departement == query_param["code_departement"]
    assert event.code_region == query_param["code_region"]
    assert event.code_commune == query_param["code_commune"]
    assert event.frais == query_param["frais"]
    assert event.modes_accueil == query_param["modes_accueil"]
    assert event.types == query_param["types"]
    assert event.recherche_public == query_param["recherche_public"]
    assert event.score_qualite_minimum == query_param["score_qualite_minimum"]
    if schema_version == "v0":
        assert event.profils == query_param["profils"]
    elif schema_version == "v1":
        assert event.publics == query_param["publics"]


@pytest.mark.parametrize(
    ("schema_version", "path", "model"),
    [
        ("v0", "/structures", v0.ListStructuresEvent),
        ("v1", "/structures", v1.ListStructuresEvent),
    ],
)
@pytest.mark.with_token
def test_list_structures_event_saved(
    api_client, db_session, url, schema_version, model
):
    query_param = {
        "sources": ["foo"],
        "code_departement": "26",
        "code_region": "84",
        "code_commune": "26400",
        "id": "1",
        "exclure_doublons": True,
    }
    if schema_version == "v0":
        query_param["thematiques"] = ["famille--garde-denfants"]
        query_param["label_national"] = "action-logement"
        query_param["typologie"] = "ACIPHC"
    elif schema_version == "v1":
        query_param["reseaux_porteurs"] = ["action-logement"]

    response = api_client.get(url, params=query_param)

    assert response.status_code == 200
    assert db_session.scalar(sqla.select(sqla.func.count()).select_from(model)) == 1

    event = db_session.scalars(sqla.select(model)).first()
    assert event.user == "some_user"
    assert event.sources == query_param["sources"]
    assert event.code_departement == query_param["code_departement"]
    assert event.code_region == query_param["code_region"]
    assert event.code_commune == query_param["code_commune"]
    assert event.exclure_doublons == query_param["exclure_doublons"]
    if schema_version == "v0":
        assert event.thematiques == query_param["thematiques"]
        assert event.label_national == query_param["label_national"]
        assert event.typologie == query_param["typologie"]
    elif schema_version == "v1":
        assert event.reseaux_porteurs == query_param["reseaux_porteurs"]


@pytest.mark.parametrize(
    ("schema_version", "path", "model"),
    [
        ("v0", "/search/services", v0.SearchServicesEvent),
        ("v1", "/search/services", v1.SearchServicesEvent),
    ],
)
@pytest.mark.with_token
def test_search_services_event_saved(
    api_client, db_session, url, schema_version, model
):
    query_param = {
        "sources": ["foo"],
        "thematiques": ["famille--garde-denfants"],
        "code_commune": HAZEBROUCK["code_insee"],
        "code_insee": DUNKERQUE["code_insee"],
        "frais": ["gratuit"],
        "modes_accueil": ["a-distance"],
        "types": ["accompagnement"],
        "lat": 45.0,
        "lon": 5.0,
        "exclure_doublons": "strict" if schema_version == "v1" else True,
        "recherche_public": "test",
        "score_qualite_minimum": 0.5,
    }
    if schema_version == "v0":
        query_param["profils"] = ["etudiants"]
        query_param["profils_precisions"] = "test"
    elif schema_version == "v1":
        query_param["publics"] = ["etudiants"]
        query_param["publics_precisions"] = "test"

    response = api_client.get(url, params=query_param)

    assert response.status_code == 200
    assert db_session.scalar(sqla.select(sqla.func.count()).select_from(model)) == 1

    event = db_session.scalars(sqla.select(model)).first()

    assert event.user == "some_user"
    assert len(event.first_services) == 0
    assert event.total_services == 0
    assert event.sources == query_param["sources"]
    assert event.thematiques == query_param["thematiques"]
    assert event.code_commune == query_param["code_commune"]
    assert event.lat == query_param["lat"]
    assert event.lon == query_param["lon"]
    assert event.frais == query_param["frais"]
    assert event.modes_accueil == query_param["modes_accueil"]
    assert event.types == query_param["types"]
    assert event.recherche_public == query_param["recherche_public"]
    assert event.score_qualite_minimum == query_param["score_qualite_minimum"]
    assert event.exclure_doublons == query_param["exclure_doublons"]
    if schema_version == "v0":
        assert event.profils == query_param["profils"]
    elif schema_version == "v1":
        assert event.publics == query_param["publics"]


@pytest.mark.parametrize(
    ("schema_version", "path", "model"),
    [
        ("v0", "/search/services", v0.SearchServicesEvent),
        ("v1", "/search/services", v1.SearchServicesEvent),
    ],
)
@pytest.mark.with_token
def test_search_services_event_saved_with_results(
    api_client, db_session, url, service_factory, model
):
    number_of_results_to_saved = 10

    for _ in range(number_of_results_to_saved + 1):
        service_factory()

    response = api_client.get(url)

    assert response.status_code == 200
    assert db_session.scalar(sqla.select(sqla.func.count()).select_from(model)) == 1

    event = db_session.scalars(sqla.select(model)).first()
    assert event.user == "some_user"
    assert event.first_services == [
        {
            "id": item["service"]["id"],
            "score_qualite": item["service"]["score_qualite"],
            "distance": item["distance"],
        }
        for item in response.json()["items"][:number_of_results_to_saved]
    ]
    assert event.total_services == response.json()["total"]


@pytest.mark.parametrize(
    ("schema_version", "path", "model"),
    [
        ("v0", "/search/services", v0.SearchServicesEvent),
        ("v1", "/search/services", v1.SearchServicesEvent),
    ],
)
@pytest.mark.with_token
def test_search_services_event_only_first_page_saved(
    api_client, db_session, url, service_factory, model
):
    service_factory()
    service_factory()

    first_response = api_client.get(url, params={"size": 1, "page": 1})
    api_client.get(url, params={"size": 1, "page": 2})

    assert db_session.scalar(sqla.select(sqla.func.count()).select_from(model)) == 1

    event = db_session.scalars(sqla.select(model)).first()
    assert event.first_services == [
        {
            "id": item["service"]["id"],
            "score_qualite": item["service"]["score_qualite"],
            "distance": item["distance"],
        }
        for item in first_response.json()["items"]
    ]
