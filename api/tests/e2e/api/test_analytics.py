import pytest
import sqlalchemy as sqla

from data_inclusion.api.analytics import models

from ... import factories

DUNKERQUE = {"code_insee": "59183", "latitude": 51.0361, "longitude": 2.3770}
HAZEBROUCK = {"code_insee": "59295", "latitude": 50.7262, "longitude": 2.5387}


@pytest.mark.parametrize(
    ("url", "model"),
    [
        ("/api/v0/structures/foo/bar", models.ConsultStructureEvent),
        ("/api/v0/services/foo/bar", models.ConsultServiceEvent),
    ],
)
@pytest.mark.with_token
def test_no_save_api_analytics_consult_if_not_found(api_client, db_session, url, model):
    response = api_client.get(url)

    assert response.status_code == 404
    assert db_session.scalar(sqla.select(sqla.func.count()).select_from(model)) == 0


@pytest.mark.with_token
def test_save_api_analytics_consult_structure_event_with_token(api_client, db_session):
    structure = factories.StructureFactory(source="foo", id="1")
    url = f"/api/v0/structures/{structure.source}/{structure.id}"
    response = api_client.get(url)

    assert response.status_code == 200
    assert (
        db_session.scalar(
            sqla.select(sqla.func.count()).select_from(models.ConsultStructureEvent)
        )
        == 1
    )

    structure_stats_view_instance = db_session.scalars(
        sqla.select(models.ConsultStructureEvent)
    ).first()
    assert structure_stats_view_instance.user == "some_user"
    assert structure_stats_view_instance.structure_id == structure.id
    assert structure_stats_view_instance.source == structure.source


@pytest.mark.with_token
def test_save_api_analytics_consult_service_with_token(api_client, db_session):
    service = factories.ServiceFactory(source="foo", id="1", score_qualite=0.8)
    url = f"/api/v0/services/{service.source}/{service.id}"
    response = api_client.get(url)

    assert response.status_code == 200
    assert (
        db_session.scalar(
            sqla.select(sqla.func.count()).select_from(models.ConsultServiceEvent)
        )
        == 1
    )

    structure_stats_view_instance = db_session.scalars(
        sqla.select(models.ConsultServiceEvent)
    ).first()
    assert structure_stats_view_instance.user == "some_user"
    assert structure_stats_view_instance.service_id == service.id
    assert structure_stats_view_instance.source == service.source
    assert structure_stats_view_instance.score_qualite == service.score_qualite


@pytest.mark.with_token
def test_save_api_analytics_resources_search_with_token_and_resources_in_db(
    api_client, db_session
):
    total_services = 11
    for _ in range(total_services):
        factories.ServiceFactory()

    response = api_client.get("/api/v0/search/services")
    # retrieve ids and score_qualite from response
    first_services_ids = [
        {"id": item["service"]["id"], "score_qualite": item["service"]["score_qualite"]}
        for item in response.json()["items"][:10]
    ]

    assert response.status_code == 200
    assert (
        db_session.scalar(
            sqla.select(sqla.func.count()).select_from(models.ServicesSearch)
        )
        == 1
    )

    service_stats_view_instance = db_session.scalars(
        sqla.select(models.ServicesSearch)
    ).first()
    assert service_stats_view_instance.user == "some_user"
    assert (
        service_stats_view_instance.first_services_ids_and_quality_score
        == first_services_ids
    )
    assert service_stats_view_instance.total_services_count == total_services


@pytest.mark.with_token
def test_save_api_analytics_service_list_event_with_token_with_query_param(
    api_client, db_session
):
    url = "/api/v0/services"
    query_param = {
        "sources": ["foo"],
        "thematiques": ["acces-aux-droits-et-citoyennete"],
        "code_departement": "26",
        "code_region": "84",
        "code_commune": "26400",
        "frais": ["gratuit"],
        "profils": ["adultes"],
        "modes_accueil": ["a-distance"],
        "types": ["accompagnement"],
        "inclure_suspendus": 1,
    }
    response = api_client.get(url, params=query_param)

    assert response.status_code == 200
    assert (
        db_session.scalar(
            sqla.select(sqla.func.count()).select_from(models.ListServicesEvent)
        )
        == 1
    )

    service_stats_view_instance = db_session.scalars(
        sqla.select(models.ListServicesEvent)
    ).first()
    assert service_stats_view_instance.user == "some_user"
    assert service_stats_view_instance.sources == query_param["sources"]
    assert service_stats_view_instance.thematiques == query_param["thematiques"]
    assert service_stats_view_instance.departement == query_param["code_departement"]
    assert service_stats_view_instance.region == query_param["code_region"]
    assert service_stats_view_instance.code_commune == query_param["code_commune"]
    assert service_stats_view_instance.frais == query_param["frais"]
    assert service_stats_view_instance.profils == query_param["profils"]
    assert service_stats_view_instance.modes_accueil == query_param["modes_accueil"]
    assert service_stats_view_instance.types == query_param["types"]
    assert (
        service_stats_view_instance.inclure_suspendus
        == query_param["inclure_suspendus"]
    )


@pytest.mark.with_token
def test_save_api_analytics_structure_list_event_with_token_with_query_param(
    api_client, db_session
):
    url = "/api/v0/structures"
    query_param = {
        "sources": ["foo"],
        "thematiques": ["acces-aux-droits-et-citoyennete"],
        "code_departement": "26",
        "code_region": "84",
        "code_commune": "26400",
        "typologie": "ACIPHC",
        "id": "1",
        "label_national": "action-logement",
    }
    response = api_client.get(url, params=query_param)

    assert response.status_code == 200
    assert (
        db_session.scalar(
            sqla.select(sqla.func.count()).select_from(models.ListStructuresEvent)
        )
        == 1
    )

    structure_stats_view_instance = db_session.scalars(
        sqla.select(models.ListStructuresEvent)
    ).first()
    assert structure_stats_view_instance.user == "some_user"
    assert structure_stats_view_instance.sources == query_param["sources"]
    assert structure_stats_view_instance.thematiques == query_param["thematiques"]
    assert structure_stats_view_instance.departement == query_param["code_departement"]
    assert structure_stats_view_instance.region == query_param["code_region"]
    assert structure_stats_view_instance.code_commune == query_param["code_commune"]
    assert structure_stats_view_instance.typologie == query_param["typologie"]
    assert structure_stats_view_instance.structure_id == query_param["id"]
    assert structure_stats_view_instance.label_national == query_param["label_national"]


@pytest.mark.with_token
def test_save_api_analytics_service_search_with_token_with_query_param(
    api_client, db_session
):
    url = "/api/v0/search/services"
    query_param = {
        "sources": ["foo"],
        "thematiques": ["acces-aux-droits-et-citoyennete"],
        "code_commune": HAZEBROUCK["code_insee"],
        "code_insee": DUNKERQUE["code_insee"],
        "frais": ["gratuit"],
        "profils": ["adultes"],
        "profils_precisions": "test",
        "modes_accueil": ["a-distance"],
        "types": ["accompagnement"],
        "inclure_suspendus": True,
        "lat": 45.0,
        "lon": 5.0,
    }
    response = api_client.get(url, params=query_param)

    assert response.status_code == 200
    assert (
        db_session.scalar(
            sqla.select(sqla.func.count()).select_from(models.ServicesSearch)
        )
        == 1
    )

    service_stats_view_instance = db_session.scalars(
        sqla.select(models.ServicesSearch)
    ).first()

    assert service_stats_view_instance.user == "some_user"
    assert len(service_stats_view_instance.first_services_ids_and_quality_score) == 0
    assert service_stats_view_instance.total_services_count == 0
    assert service_stats_view_instance.sources == query_param["sources"]
    assert service_stats_view_instance.thematiques == query_param["thematiques"]
    assert service_stats_view_instance.code_commune == query_param["code_commune"]
    assert service_stats_view_instance.lat == query_param["lat"]
    assert service_stats_view_instance.lon == query_param["lon"]
    assert service_stats_view_instance.frais == query_param["frais"]
    assert service_stats_view_instance.profils == query_param["profils"]
    assert service_stats_view_instance.modes_accueil == query_param["modes_accueil"]
    assert service_stats_view_instance.types == query_param["types"]
    assert (
        service_stats_view_instance.inclure_suspendus
        == query_param["inclure_suspendus"]
    )
