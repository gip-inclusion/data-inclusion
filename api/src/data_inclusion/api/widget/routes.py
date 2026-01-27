import fnmatch
from pathlib import Path
from typing import Annotated

import furl
import pydantic
from starlette.authentication import AuthCredentials, SimpleUser

import fastapi
from fastapi import staticfiles, templating

from data_inclusion.api.analytics.v1.services import save_event
from data_inclusion.api.auth.services import verify_token
from data_inclusion.api.config import settings
from data_inclusion.api.core import db
from data_inclusion.api.decoupage_administratif.models import Commune
from data_inclusion.api.inclusion_data.v1 import parameters, services
from data_inclusion.api.utils import pagination
from data_inclusion.schema import v1

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_FILES_DIR = Path(__file__).parent / "static"

app = fastapi.FastAPI()
app.mount(
    path="/static",
    app=staticfiles.StaticFiles(directory=STATIC_FILES_DIR.resolve()),
    name="static",
)

templates = templating.Jinja2Templates(directory=TEMPLATES_DIR)


def thematiques_filter(
    thematiques: list[str] | None, selected_category: str | None = None
) -> list[str]:
    if not thematiques:
        return []

    def _category_from_prefix(prefix: str) -> str:
        for cat in v1.Categorie:
            if cat.value == prefix:
                return cat.label
        return prefix

    prefixes = set()
    for t in thematiques:
        if "--" in t:
            prefixes.add(t.split("--")[0])

    sorted_prefixes = sorted(prefixes)

    if selected_category and selected_category in sorted_prefixes:
        sorted_prefixes.remove(selected_category)
        sorted_prefixes = [selected_category] + sorted_prefixes

    result = []
    for p in sorted_prefixes[:3]:
        result.append(_category_from_prefix(p))

    return result


templates.env.filters["thematiques_filter"] = thematiques_filter


def validate_widget_token(request: fastapi.Request, token: str) -> str | None:
    payload = verify_token(token)
    if payload is None:
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_403_FORBIDDEN,
            detail="Invalid widget token.",
        )

    allowed_origins = payload.get("allowed_origins")
    if allowed_origins is None:
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_403_FORBIDDEN,
            detail="Widget access not configured for this token.",
        )

    if "*" in allowed_origins:
        return payload.get("sub")

    req_origin = request.headers.get("origin") or request.headers.get("referer")
    if req_origin is None:
        request_url = str(request.url)
        req_origin = furl.furl(request_url).origin

    origin = furl.furl(req_origin).origin
    origin_host = furl.furl(origin).host

    if origin_host in ("localhost", "127.0.0.1"):
        return payload.get("sub")

    widget_host_origin = furl.furl(settings.BASE_URL).origin
    if origin == widget_host_origin:
        return payload.get("sub")

    if not any(fnmatch.fnmatch(origin, p) for p in allowed_origins):
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_403_FORBIDDEN,
            detail="Origin not allowed for this token.",
        )

    return payload.get("sub")


def get_results(
    db_session,
    size: int,
    score_qualite_minimum: float,
    publics: list[v1.Public] | None = None,
    thematiques: list[v1.Thematique] | None = None,
    page: int = 1,
    code_commune: str | None = None,
    sources: list[str] | None = None,
    include_remote_services: bool = False,
):
    params = parameters.SearchServicesQueryParams(
        code_commune=code_commune or None,
        thematiques=thematiques,
        publics=publics,
        score_qualite_minimum=score_qualite_minimum,
        exclure_doublons=True,
        sources=sources,
    )

    commune_instance = None
    if params.code_commune is not None:
        commune_instance = db_session.get(Commune, params.code_commune)
        if commune_instance is None:
            raise fastapi.HTTPException(
                status_code=fastapi.status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="This `code_commune` does not exist.",
            )

    query, mapping = services.search_services_query(
        params=params,
        include_soliguide=False,
        include_remote_services=include_remote_services or code_commune is None,
        commune_instance=commune_instance,
    )

    return pagination.paginate(
        db_session=db_session,
        query=query,
        size=size,
        page=page,
        mapping=mapping,
    )


def list_thematiques(categories: list[v1.Categorie]) -> list[v1.Thematique]:
    return sorted(
        t for t in v1.Thematique if any(t.value.startswith(c.value) for c in categories)
    )


@app.get(path="/")
def widget(
    request: fastapi.Request,
    background_tasks: fastapi.BackgroundTasks,
    token: Annotated[
        str,
        fastapi.Query(description="Token d'authentification du widget (obligatoire)"),
    ],
    display_form: Annotated[
        bool,
        fastapi.Query(description="Affiche ou masque le formulaire de recherche"),
    ] = True,
    display_communes_filter: Annotated[
        bool,
        fastapi.Query(description="Affiche ou masque le filtre par commune"),
    ] = True,
    display_categories_filter: Annotated[
        bool,
        fastapi.Query(
            description="Affiche ou masque le filtre par catégorie/thématique"
        ),
    ] = True,
    display_publics_filter: Annotated[
        bool,
        fastapi.Query(description="Affiche ou masque le filtre par public cible"),
    ] = True,
    code_commune: Annotated[
        str | None,
        fastapi.Query(
            description="Code INSEE de la commune pour pré-filtrer les résultats"
        ),
    ] = None,
    categories: Annotated[
        list[v1.Categorie] | None,
        fastapi.Query(description="Catégories pour pré-filtrer les résultats"),
    ] = None,
    thematiques: Annotated[
        list[v1.Thematique] | None,
        fastapi.Query(
            description="Thématiques pour pré-filtrer. "
            "Si fourni, les catégories sont ignorées et le filtre catégories"
            " est masqué."
        ),
    ] = None,
    publics: Annotated[
        list[v1.Public] | None,
        fastapi.Query(
            description="Publics cibles pour pré-filtrer les résultats",
        ),
    ] = None,
    score_qualite_minimum: Annotated[
        float,
        fastapi.Query(
            description="Score de qualité minimum des services (0 à 1)",
        ),
    ] = 0.8,
    sources: Annotated[
        list[str] | None,
        fastapi.Query(
            description="Liste blanche d'identifiants de sources à inclure",
        ),
    ] = None,
    include_remote_services: Annotated[
        bool,
        fastapi.Query(
            description="Inclure les services en ligne (sans commune associée)"
        ),
    ] = False,
    size: Annotated[
        int | None,
        fastapi.Query(
            description="Nombre de services par ligne, sans pagination. "
            "Si non défini, utilise la grille x*y."
        ),
    ] = None,
    x: Annotated[
        int,
        fastapi.Query(description="Nombre de colonnes de résultats"),
    ] = 5,
    y: Annotated[
        int,
        fastapi.Query(description="Nombre de lignes de résultats"),
    ] = 2,
    hx_request: Annotated[bool, fastapi.Header()] = False,
    page: Annotated[int, pydantic.Field(ge=1)] = 1,
    db_session=fastapi.Depends(db.get_session),
) -> fastapi.responses.HTMLResponse:
    if settings.TOKEN_ENABLED:
        token_sub = validate_widget_token(request=request, token=token)
    else:
        token_sub = "test_user"

    request.scope["user"] = SimpleUser(username=token_sub)
    request.scope["auth"] = AuthCredentials(scopes=["authenticated"])

    commune_instance = None
    if code_commune:
        commune_instance = db_session.get(Commune, code_commune)

    if thematiques:
        display_categories_filter = False
        thematiques_for_query = thematiques
    else:
        thematiques_for_query = list_thematiques(categories) if categories else None

    inline_mode = size is not None
    effective_size = size if size is not None else x * y
    results = get_results(
        db_session=db_session,
        page=page,
        code_commune=code_commune,
        publics=publics,
        thematiques=thematiques_for_query,
        score_qualite_minimum=score_qualite_minimum,
        size=effective_size,
        sources=sources,
        include_remote_services=include_remote_services,
    )

    params = parameters.SearchServicesQueryParams(
        code_commune=code_commune or None,
        thematiques=thematiques_for_query,
        publics=publics,
        score_qualite_minimum=score_qualite_minimum,
        exclure_doublons=True,
        sources=sources,
    )
    background_tasks.add_task(
        save_event,
        request=request,
        params=params,
        db_session=db_session,
        first_results_page=results,
    )

    current_url = str(request.url)

    commune_label = None
    if commune_instance is not None:
        commune_label = commune_instance.nom

    if hx_request:
        return templates.TemplateResponse(
            request=request,
            name="fragments/results.html",
            context={
                "request": request,
                "token": token,
                "token_sub": token_sub,
                "current_url": current_url,
                "schema_publics": v1.Public,
                "query_params_categories": categories,
                "commune_label": commune_label,
                "results": results["items"],
                "page": results["page"],
                "pages": results["pages"],
                "x": x,
                "inline_mode": inline_mode,
            },
        )

    return templates.TemplateResponse(
        request=request,
        name="widget.html",
        context={
            "request": request,
            "token": token,
            "token_sub": token_sub,
            "token_enabled": settings.TOKEN_ENABLED,
            "current_url": current_url,
            "schema_categories": v1.Categorie,
            "schema_publics": v1.Public,
            "query_params_commune_code": code_commune,
            "query_params_commune_label": commune_label,
            "commune_label": commune_label,
            "query_params_publics": publics,
            "query_params_categories": categories,
            "query_params_x": x,
            "query_params_y": y,
            "query_params_score_qualite_minimum": score_qualite_minimum,
            "query_params_display_form": display_form,
            "query_params_display_communes_filter": display_communes_filter,
            "query_params_display_categories_filter": display_categories_filter,
            "query_params_display_publics_filter": display_publics_filter,
            "query_params_sources": sources,
            "query_params_thematiques": thematiques,
            "query_params_include_remote_services": include_remote_services,
            "results": results["items"],
            "display_form": display_form,
            "display_communes_filter": display_communes_filter,
            "display_categories_filter": display_categories_filter,
            "display_publics_filter": display_publics_filter,
            "page": results["page"],
            "pages": results["pages"],
            "x": x,
            "inline_mode": inline_mode,
        },
    )


@app.get(path="/example")
def widget_example(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="example.html",
    )
