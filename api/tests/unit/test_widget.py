import pytest

import fastapi

from data_inclusion.api import auth
from data_inclusion.api.widget.routes import validate_widget_token
from data_inclusion.schema import v1

from .. import factories


class MockRequest:
    def __init__(self, headers=None, url=None):
        self.headers = headers or {}
        self.url = url or "https://api.data.inclusion.gouv.fr/widget/"


def test_widget_token_validation_invalid_token():
    request = MockRequest(headers={})
    with pytest.raises(fastapi.HTTPException) as exc_info:
        validate_widget_token(request, "invalid-token")

    assert exc_info.value.status_code == 403
    assert exc_info.value.detail == "Invalid widget token."


@pytest.mark.parametrize(
    (
        "allowed_origins",
        "request_headers",
        "status_code",
        "detail",
    ),
    [
        (
            None,
            {},
            403,
            "Widget access not configured for this token.",
        ),
        (
            ["https://mairie.arras.fr"],
            {},
            403,
            "Origin not allowed for this token.",
        ),
        (
            ["https://api.data.inclusion.gouv.fr"],
            {},
            200,
            None,
        ),
        (
            ["https://mairie.arras.fr"],
            {"origin": "https://attaquant.com"},
            403,
            "Origin not allowed for this token.",
        ),
        (
            ["https://mairie.arras.fr"],
            {"origin": "https://mairie.arras.fr"},
            200,
            None,
        ),
        (
            ["https://*.arras.fr"],
            {"origin": "https://tartempion.arras.fr"},
            200,
            None,
        ),
        (["*"], {}, 200, None),
        (
            ["https://mairie.arras.fr"],
            {"referer": "https://mairie.arras.fr/page"},
            200,
            None,
        ),
        (
            ["https://some-other-domain.com"],
            {"origin": "http://testserver"},
            200,
            None,
        ),
    ],
)
def test_widget_token_validation(allowed_origins, request_headers, status_code, detail):
    token = auth.create_access_token("mairie-arras", allowed_origins=allowed_origins)

    request = MockRequest(headers=request_headers)

    if status_code == 200:
        validate_widget_token(request, token)
    else:
        with pytest.raises(fastapi.HTTPException) as exc_info:
            validate_widget_token(request, token)

        assert exc_info.value.status_code == status_code
        assert exc_info.value.detail == detail


def test_widget_rendering_empty_results(api_client, snapshot, auth_disabled):  # noqa: ARG001
    response = api_client.get("/widget/?token=test-token&x=2&y=1")
    assert response.status_code == 200
    assert response.text.strip() == snapshot


def test_widget_rendering_with_results(api_client, db_session, snapshot, auth_disabled):  # noqa: ARG001
    factories.v1.ServiceFactory(
        source="dora",
        structure__nom="Structure Formation",
        nom="Service de formation professionnelle",
        commune="Lille",
        code_postal="59000",
        code_insee="59350",
        latitude=50.6292,
        longitude=3.0573,
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
        publics=[v1.Public.FEMMES.value],
        thematiques=[v1.Thematique.FAMILLE__GARDE_DENFANTS.value],
        score_qualite=0.9,
    )
    factories.v1.ServiceFactory(
        source="dora",
        structure__nom="Structure Emploi",
        nom="Accompagnement à l'emploi",
        commune="Paris",
        code_postal="75001",
        code_insee="75056",
        latitude=48.8566,
        longitude=2.3522,
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
        publics=[v1.Public.JEUNES.value],
        thematiques=[
            v1.Thematique.MOBILITE__ETRE_ACCOMPAGNE_DANS_SON_PARCOURS_MOBILITE.value
        ],
        score_qualite=0.85,
    )
    factories.v1.ServiceFactory(
        source="emplois-de-linclusion",
        structure__nom="Structure Sans Commune",
        nom="Service sans commune",
        commune=None,
        code_postal=None,
        latitude=None,
        longitude=None,
        modes_accueil=[v1.ModeAccueil.A_DISTANCE.value],
        publics=[v1.Public.SENIORS.value],
        thematiques=[v1.Thematique.FAMILLE__GARDE_DENFANTS.value],
        score_qualite=0.8,
    )

    response = api_client.get("/widget/?token=test-token&x=2&y=1")
    assert response.status_code == 200
    assert response.text.strip() == snapshot(name="render all results")

    response = api_client.get(
        "/widget/?token=test-token&x=2&y=1&code_commune=59350&categories=famille",
        headers={"HX-Request": "true"},
    )
    assert response.status_code == 200
    assert response.text.strip() == snapshot(name="HTMX results for Lille & famille")

    response = api_client.get(
        "/widget/?token=test-token&x=2&y=1&code_commune=75056&categories=famille",
        headers={"HX-Request": "true"},
    )
    assert response.status_code == 200
    assert response.text.strip() == snapshot(name="HTMX results for Paris & famille")


def test_widget_filter_sources(api_client, db_session, auth_disabled):  # noqa: ARG001
    factories.v1.ServiceFactory(
        source="dora",
        structure__nom="Structure Dora",
        nom="Service Dora",
        commune="Lille",
        code_postal="59000",
        code_insee="59350",
        score_qualite=0.9,
    )
    factories.v1.ServiceFactory(
        source="emplois-de-linclusion",
        structure__nom="Structure Emplois",
        nom="Service Emplois",
        commune="Lille",
        code_postal="59000",
        code_insee="59350",
        score_qualite=0.9,
    )
    factories.v1.ServiceFactory(
        source="mes-aides",
        structure__nom="Structure MesAides",
        nom="Service MesAides",
        commune="Lille",
        code_postal="59000",
        code_insee="59350",
        score_qualite=0.9,
    )

    response = api_client.get("/widget/?token=test-token&sources=dora")
    assert response.status_code == 200
    assert "Service Dora" in response.text
    assert "Service Emplois" not in response.text
    assert "Service MesAides" not in response.text

    response = api_client.get(
        "/widget/?token=test-token&sources=dora&sources=mes-aides"
    )
    assert response.status_code == 200
    assert "Service Dora" in response.text
    assert "Service Emplois" not in response.text
    assert "Service MesAides" in response.text


def test_widget_filter_thematiques(api_client, db_session, auth_disabled):  # noqa: ARG001
    factories.v1.ServiceFactory(
        source="dora",
        structure__nom="Structure Famille",
        nom="Service Garde Enfants",
        commune="Lille",
        code_postal="59000",
        code_insee="59350",
        thematiques=[v1.Thematique.FAMILLE__GARDE_DENFANTS.value],
        score_qualite=0.9,
    )
    factories.v1.ServiceFactory(
        source="dora",
        structure__nom="Structure Mobilite",
        nom="Service Mobilite",
        commune="Lille",
        code_postal="59000",
        code_insee="59350",
        thematiques=[
            v1.Thematique.MOBILITE__ETRE_ACCOMPAGNE_DANS_SON_PARCOURS_MOBILITE.value
        ],
        score_qualite=0.9,
    )
    factories.v1.ServiceFactory(
        source="dora",
        structure__nom="Structure Logement",
        nom="Service Logement",
        commune="Lille",
        code_postal="59000",
        code_insee="59350",
        thematiques=[v1.Thematique.LOGEMENT_HEBERGEMENT__LOUER_UN_LOGEMENT.value],
        score_qualite=0.9,
    )

    response = api_client.get(
        "/widget/?token=test-token&thematiques=famille--garde-denfants"
    )
    assert response.status_code == 200
    assert "Service Garde Enfants" in response.text
    assert "Service Mobilite" not in response.text
    assert "Service Logement" not in response.text

    response = api_client.get(
        "/widget/?token=test-token"
        "&thematiques=famille--garde-denfants"
        "&thematiques=logement-hebergement--louer-un-logement"
    )
    assert response.status_code == 200
    assert "Service Garde Enfants" in response.text
    assert "Service Mobilite" not in response.text
    assert "Service Logement" in response.text


def test_widget_filter_include_online_services(api_client, db_session, auth_disabled):  # noqa: ARG001
    factories.v1.ServiceFactory(
        source="dora",
        structure__nom="Structure Presentiel",
        nom="Service Presentiel",
        commune="Lille",
        code_postal="59000",
        code_insee="59350",
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
        score_qualite=0.9,
    )
    factories.v1.ServiceFactory(
        source="dora",
        structure__nom="Structure En Ligne",
        nom="Service En Ligne",
        commune=None,
        code_postal=None,
        code_insee=None,
        latitude=None,
        longitude=None,
        modes_accueil=[v1.ModeAccueil.A_DISTANCE.value],
        score_qualite=0.9,
    )

    response = api_client.get("/widget/?token=test-token")
    assert response.status_code == 200
    assert "Service Presentiel" in response.text
    assert "Service En Ligne" not in response.text

    response = api_client.get("/widget/?token=test-token&include_online_services=true")
    assert response.status_code == 200
    assert "Service Presentiel" in response.text
    assert "Service En Ligne" in response.text

    response = api_client.get("/widget/?token=test-token&include_online_services=false")
    assert response.status_code == 200
    assert "Service Presentiel" in response.text
    assert "Service En Ligne" not in response.text


def test_widget_filter_publics(api_client, db_session, auth_disabled):  # noqa: ARG001
    factories.v1.ServiceFactory(
        source="dora",
        structure__nom="Structure Femmes",
        nom="Service Pour Femmes",
        commune="Lille",
        code_postal="59000",
        code_insee="59350",
        publics=[v1.Public.FEMMES.value],
        score_qualite=0.9,
    )
    factories.v1.ServiceFactory(
        source="dora",
        structure__nom="Structure Jeunes",
        nom="Service Pour Jeunes",
        commune="Lille",
        code_postal="59000",
        code_insee="59350",
        publics=[v1.Public.JEUNES.value],
        score_qualite=0.9,
    )
    factories.v1.ServiceFactory(
        source="dora",
        structure__nom="Structure Seniors",
        nom="Service Pour Seniors",
        commune="Lille",
        code_postal="59000",
        code_insee="59350",
        publics=[v1.Public.SENIORS.value],
        score_qualite=0.9,
    )

    response = api_client.get("/widget/?token=test-token&publics=femmes")
    assert response.status_code == 200
    assert "Service Pour Femmes" in response.text
    assert "Service Pour Jeunes" not in response.text
    assert "Service Pour Seniors" not in response.text

    response = api_client.get(
        "/widget/?token=test-token&publics=femmes&publics=seniors"
    )
    assert response.status_code == 200
    assert "Service Pour Femmes" in response.text
    assert "Service Pour Jeunes" not in response.text
    assert "Service Pour Seniors" in response.text
