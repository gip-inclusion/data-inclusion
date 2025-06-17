import pytest
import sqlalchemy as sqla

from data_inclusion.api.analytics import models

DUNKERQUE = {"code_insee": "59183", "latitude": 51.0361, "longitude": 2.3770}
HAZEBROUCK = {"code_insee": "59295", "latitude": 50.7262, "longitude": 2.5387}


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize(
    ("path", "model"),
    [
        ("/structures/foo/bar", models.ConsultStructureEvent),
        ("/services/foo/bar", models.ConsultServiceEvent),
    ],
)
@pytest.mark.with_token
def test_ignore_event_if_resource_not_found(api_client, db_session, url, model):
    response = api_client.get(url)

    assert response.status_code == 404
    assert db_session.scalar(sqla.select(sqla.func.count()).select_from(model)) == 0


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures"])
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
    schema_version,
    user_agent,
    num_events,
    structure_factory,
):
    structure = structure_factory(source="foo", id="1")
    url = f"{url}/{structure.source}/{structure.id}"

    response = api_client.get(
        url,
        headers={
            "User-Agent": user_agent,
        },
    )

    assert response.status_code == 200
    assert (
        db_session.scalar(
            sqla.select(sqla.func.count())
            .select_from(models.ConsultStructureEvent)
            .filter_by(schema_version=schema_version)
        )
        == num_events
    )

    event = db_session.scalars(sqla.select(models.ConsultStructureEvent)).first()
    if event:
        assert event.user == "some_user"
        assert event.structure_id == structure.id
        assert event.source == structure.source


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services"])
@pytest.mark.parametrize(
    ("user_agent", "num_events"),
    [
        ("bingbot/2.0; +http://www.bing.com/bingbot.htm)", 0),
        ("python-requests/2.28.1", 1),
    ],
)
@pytest.mark.with_token
def test_consult_service_event_saved(
    api_client, db_session, url, schema_version, user_agent, num_events, service_factory
):
    service = service_factory(source="foo", id="1", score_qualite=0.8)
    url = f"{url}/{service.source}/{service.id}"

    response = api_client.get(
        url,
        headers={
            "User-Agent": user_agent,
        },
    )

    assert response.status_code == 200
    assert (
        db_session.scalar(
            sqla.select(sqla.func.count())
            .select_from(models.ConsultServiceEvent)
            .filter_by(schema_version=schema_version)
        )
        == num_events
    )

    event = db_session.scalars(sqla.select(models.ConsultServiceEvent)).first()
    if event:
        assert event.user == "some_user"
        assert event.service_id == service.id
        assert event.source == service.source
        assert event.score_qualite == service.score_qualite


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services"])
@pytest.mark.with_token
def test_list_services_event_saved(
    api_client, db_session, url, schema_version, publics_param
):
    query_param = {
        "sources": ["foo"],
        "thematiques": ["acces-aux-droits-et-citoyennete"],
        "code_departement": "26",
        "code_region": "84",
        "code_commune": "26400",
        "frais": ["gratuit"],
        publics_param: ["etudiants"],
        "modes_accueil": ["a-distance"],
        "types": ["accompagnement"],
        "recherche_public": "test",
        "score_qualite_minimum": 0.5,
    }
    response = api_client.get(url, params=query_param)

    assert response.status_code == 200
    assert (
        db_session.scalar(
            sqla.select(sqla.func.count())
            .select_from(models.ListServicesEvent)
            .filter_by(schema_version=schema_version)
        )
        == 1
    )

    event = db_session.scalars(sqla.select(models.ListServicesEvent)).first()
    assert event.user == "some_user"
    assert event.sources == query_param["sources"]
    assert event.thematiques == query_param["thematiques"]
    assert event.code_departement == query_param["code_departement"]
    assert event.code_region == query_param["code_region"]
    assert event.code_commune == query_param["code_commune"]
    assert event.frais == query_param["frais"]
    assert event.profils == query_param[publics_param]
    assert event.modes_accueil == query_param["modes_accueil"]
    assert event.types == query_param["types"]
    assert event.recherche_public == query_param["recherche_public"]
    assert event.score_qualite_minimum == query_param["score_qualite_minimum"]


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures"])
@pytest.mark.with_token
def test_list_structures_event_saved(api_client, db_session, url, schema_version):
    query_param = {
        "sources": ["foo"],
        "code_departement": "26",
        "code_region": "84",
        "code_commune": "26400",
        "typologie": "ACIPHC",
        "id": "1",
        "label_national": "action-logement",
        "exclure_doublons": True,
    }
    if schema_version == "v0":
        query_param["thematiques"] = ["acces-aux-droits-et-citoyennete"]
    response = api_client.get(url, params=query_param)

    assert response.status_code == 200
    assert (
        db_session.scalar(
            sqla.select(sqla.func.count())
            .select_from(models.ListStructuresEvent)
            .filter_by(schema_version=schema_version)
        )
        == 1
    )

    event = db_session.scalars(sqla.select(models.ListStructuresEvent)).first()
    assert event.user == "some_user"
    assert event.sources == query_param["sources"]
    assert event.code_departement == query_param["code_departement"]
    assert event.code_region == query_param["code_region"]
    assert event.code_commune == query_param["code_commune"]
    assert event.typologie == query_param["typologie"]
    assert event.label_national == query_param["label_national"]
    assert event.exclure_doublons == query_param["exclure_doublons"]
    if schema_version == "v0":
        assert event.thematiques == query_param["thematiques"]


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_event_saved(
    api_client, db_session, url, schema_version, publics_param
):
    query_param = {
        "sources": ["foo"],
        "thematiques": ["acces-aux-droits-et-citoyennete"],
        "code_commune": HAZEBROUCK["code_insee"],
        "code_insee": DUNKERQUE["code_insee"],
        "frais": ["gratuit"],
        publics_param: ["etudiants"],
        f"{publics_param}_precisions": "test",
        "modes_accueil": ["a-distance"],
        "types": ["accompagnement"],
        "lat": 45.0,
        "lon": 5.0,
        "exclure_doublons": True,
        "recherche_public": "test",
        "score_qualite_minimum": 0.5,
    }
    response = api_client.get(url, params=query_param)

    assert response.status_code == 200
    assert (
        db_session.scalar(
            sqla.select(sqla.func.count())
            .select_from(models.SearchServicesEvent)
            .filter_by(schema_version=schema_version)
        )
        == 1
    )

    event = db_session.scalars(sqla.select(models.SearchServicesEvent)).first()

    assert event.user == "some_user"
    assert len(event.first_services) == 0
    assert event.total_services == 0
    assert event.sources == query_param["sources"]
    assert event.thematiques == query_param["thematiques"]
    assert event.code_commune == query_param["code_commune"]
    assert event.lat == query_param["lat"]
    assert event.lon == query_param["lon"]
    assert event.frais == query_param["frais"]
    assert event.profils == query_param[publics_param]
    assert event.modes_accueil == query_param["modes_accueil"]
    assert event.types == query_param["types"]
    assert event.recherche_public == query_param["recherche_public"]
    assert event.score_qualite_minimum == query_param["score_qualite_minimum"]
    assert event.exclure_doublons == query_param["exclure_doublons"]


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_event_saved_with_results(
    api_client, db_session, url, schema_version, service_factory
):
    number_of_results_to_saved = 10

    for _ in range(number_of_results_to_saved + 1):
        service_factory()

    response = api_client.get(url)

    assert response.status_code == 200
    assert (
        db_session.scalar(
            sqla.select(sqla.func.count())
            .select_from(models.SearchServicesEvent)
            .filter_by(schema_version=schema_version)
        )
        == 1
    )

    event = db_session.scalars(sqla.select(models.SearchServicesEvent)).first()
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


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_event_only_first_page_saved(
    api_client, db_session, url, schema_version, service_factory
):
    service_factory()
    service_factory()

    first_response = api_client.get(url, params={"size": 1, "page": 1})
    api_client.get("/api/v0/search/services", params={"size": 1, "page": 2})

    assert (
        db_session.scalar(
            sqla.select(sqla.func.count())
            .select_from(models.SearchServicesEvent)
            .filter_by(schema_version=schema_version)
        )
        == 1
    )

    event = db_session.scalars(sqla.select(models.SearchServicesEvent)).first()
    assert event.first_services == [
        {
            "id": item["service"]["id"],
            "score_qualite": item["service"]["score_qualite"],
            "distance": item["distance"],
        }
        for item in first_response.json()["items"]
    ]
