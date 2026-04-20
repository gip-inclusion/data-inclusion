import pytest
import sqlalchemy as sqla

from data_inclusion.api.analytics import models

from ... import factories

DUNKERQUE = {"code_insee": "59183", "latitude": 51.0361, "longitude": 2.3770}
HAZEBROUCK = {"code_insee": "59295", "latitude": 50.7262, "longitude": 2.5387}


@pytest.mark.parametrize(
    ("url", "model"),
    [
        ("/api/v1/structures/foo--bar", models.ConsultStructureEvent),
        ("/api/v1/services/foo--bar", models.ConsultServiceEvent),
    ],
)
@pytest.mark.with_token
def test_ignore_event_if_resource_not_found(api_client, db_session, url, model):
    response = api_client.get(url)

    assert response.status_code == 404
    assert db_session.scalar(sqla.select(sqla.func.count()).select_from(model)) == 0


@pytest.mark.parametrize(
    ("user_agent", "num_events"),
    [
        ("bingbot/2.0; +http://www.bing.com/bingbot.htm)", 0),
        ("python-requests/2.28.1", 1),
    ],
)
@pytest.mark.with_token
def test_consult_structure_event_saved(api_client, db_session, user_agent, num_events):
    structure = factories.StructureFactory(source="foo", id="foo--1")

    model = models.ConsultStructureEvent
    url = f"/api/v1/structures/{structure.id}"

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
    user_agent,
    num_events,
):
    service = factories.ServiceFactory(source="foo", id="foo--1", score_qualite=0.8)

    model = models.ConsultServiceEvent
    url = f"/api/v1/services/{service.id}"

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


@pytest.mark.with_token
def test_list_services_event_saved(api_client, db_session):
    model = models.ListServicesEvent
    url = "/api/v1/services"
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
        "publics": ["etudiants"],
        "publics_precisions": "test",
    }

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
    assert event.publics == query_param["publics"]


@pytest.mark.with_token
def test_list_structures_event_saved(api_client, db_session):
    model = models.ListStructuresEvent
    url = "/api/v1/structures"
    query_param = {
        "sources": ["foo"],
        "code_departement": "26",
        "code_region": "84",
        "code_commune": "26400",
        "id": "1",
        "exclure_doublons": True,
        "reseaux_porteurs": ["action-logement"],
    }

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
    assert event.reseaux_porteurs == query_param["reseaux_porteurs"]


@pytest.mark.with_token
def test_search_services_event_saved(api_client, db_session):
    model = models.SearchServicesEvent
    url = "/api/v1/search/services"
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
        "exclure_doublons": True,
        "recherche_public": "test",
        "score_qualite_minimum": 0.5,
        "publics": ["etudiants"],
        "publics_precisions": "test",
    }

    response = api_client.get(url, params=query_param)

    assert response.status_code == 200
    assert db_session.scalar(sqla.select(sqla.func.count()).select_from(model)) == 1

    event = db_session.scalars(sqla.select(model)).first()

    assert event.user == "some_user"
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
    assert event.publics == query_param["publics"]


@pytest.mark.with_token
def test_search_services_event_saved_with_results(api_client, db_session):
    model = models.SearchServicesEvent
    url = "/api/v1/search/services"
    number_of_results_to_saved = 10

    for _ in range(number_of_results_to_saved + 1):
        factories.ServiceFactory()

    response = api_client.get(url)

    assert response.status_code == 200
    assert db_session.scalar(sqla.select(sqla.func.count()).select_from(model)) == 1

    event = db_session.scalars(sqla.select(model)).first()
    assert event.user == "some_user"
    assert event.total_services == response.json()["total"]


@pytest.mark.with_token
def test_search_services_event_only_first_page_saved(api_client, db_session):
    model = models.SearchServicesEvent
    url = "/api/v1/search/services"
    factories.ServiceFactory()
    factories.ServiceFactory()

    first_response = api_client.get(url, params={"size": 1, "page": 1})
    api_client.get(url, params={"size": 1, "page": 2})

    assert db_session.scalar(sqla.select(sqla.func.count()).select_from(model)) == 1

    event = db_session.scalars(sqla.select(model)).first()
    assert event.total_services == first_response.json()["total"]
