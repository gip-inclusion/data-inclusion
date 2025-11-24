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


@pytest.mark.settings(TOKEN_ENABLED=False)
def test_widget_rendering_empty_results(api_client, snapshot, monkeypatch):
    from data_inclusion.api.widget.routes import settings as widget_settings

    monkeypatch.setattr(widget_settings, "TOKEN_ENABLED", False)

    response = api_client.get("/widget/?token=test-token&x=2&y=1")
    assert response.status_code == 200
    assert response.text.strip() == snapshot


@pytest.mark.settings(TOKEN_ENABLED=False)
def test_widget_rendering_with_results(api_client, db_session, snapshot, monkeypatch):
    from data_inclusion.api.widget.routes import settings as widget_settings

    monkeypatch.setattr(widget_settings, "TOKEN_ENABLED", False)

    service1 = factories.v1.ServiceFactory(
        nom="Service de formation professionnelle",
        commune="Lille",
        code_postal="59000",
        code_insee="59350",
        publics=[v1.Public.FEMMES.value],
        thematiques=[v1.Thematique.FAMILLE__GARDE_DENFANTS.value],
        score_qualite=0.9,
    )
    service2 = factories.v1.ServiceFactory(
        nom="Accompagnement à l'emploi",
        commune="Paris",
        code_postal="75001",
        code_insee="75056",
        publics=[v1.Public.JEUNES.value],
        thematiques=[
            v1.Thematique.MOBILITE__ETRE_ACCOMPAGNE_DANS_SON_PARCOURS_MOBILITE.value  # noqa: E501
        ],
        score_qualite=0.85,
    )
    service3 = factories.v1.ServiceFactory(
        nom="Service sans commune",
        commune=None,
        code_postal=None,
        publics=[v1.Public.SENIORS.value],
        thematiques=[v1.Thematique.FAMILLE__GARDE_DENFANTS.value],
        score_qualite=0.8,
    )

    db_session.add_all([service1, service2, service3])
    db_session.commit()

    # test initial request
    response = api_client.get("/widget/?token=test-token&x=2&y=1")
    assert response.status_code == 200
    assert response.text.strip() == snapshot

    # test updating the filters
    response = api_client.get(
        "/widget/?token=test-token&x=2&y=1&code_commune=59350&categories=famille",
        headers={"HX-Request": "true"},
    )
    assert response.status_code == 200
    assert response.text.strip() == snapshot

    # no results (looking in Paris & famille)
    response = api_client.get(
        "/widget/?token=test-token&x=2&y=1&code_commune=75056&categories=famille",
        headers={"HX-Request": "true"},
    )
    assert response.status_code == 200
    assert response.text.strip() == snapshot
