import fnmatch
from pathlib import Path
from typing import Annotated

import furl
import pydantic

import fastapi
from fastapi import staticfiles, templating

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
) -> list[dict]:
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
        result.append(
            {
                "label": _category_from_prefix(p),
                "is_overflow": False,
            }
        )
    if len(sorted_prefixes) > 3:
        result.append({"label": "etc.…", "is_overflow": True})

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
    if not any(fnmatch.fnmatch(origin, p) for p in allowed_origins):
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_403_FORBIDDEN,
            detail="Origin not allowed for this token.",
        )

    return payload.get("sub")


def get_results(
    db_session,
    size: int,
    publics: list[v1.Public],
    score_qualite_minimum: float,
    thematiques: list[v1.Thematique] | None = None,
    page: int = 1,
    code_commune: str | None = None,
):
    params = parameters.SearchServicesQueryParams(
        code_commune=code_commune,
        thematiques=thematiques,
        publics=publics,
        score_qualite_minimum=score_qualite_minimum,
        exclure_doublons=True,
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
        commune_instance=commune_instance,
    )

    return pagination.paginate(
        db_session=db_session,
        query=query,
        size=size,
        page=page,
        mapping=mapping,
    )


def list_thematiques(category: v1.Categorie) -> list[v1.Thematique]:
    return sorted(t for t in v1.Thematique if t.value.startswith(category.value))


@app.get(path="/")
def widget(
    request: fastapi.Request,
    token: Annotated[str, fastapi.Query(description="Widget authentication token")],
    display_form: bool = True,
    display_communes_filter: bool = True,
    display_categories_filter: bool = True,
    display_publics_filter: bool = True,
    code_commune: str | None = None,
    categories: v1.Categorie | None = None,
    publics: v1.Public = v1.Public.TOUS_PUBLICS,
    score_qualite_minimum: float = 0.8,
    x: int = 5,
    y: int = 2,
    hx_request: Annotated[bool, fastapi.Header()] = False,
    page: Annotated[int, pydantic.Field(ge=1)] = 1,
    db_session=fastapi.Depends(db.get_session),
) -> fastapi.responses.HTMLResponse:
    if settings.TOKEN_ENABLED:
        token_sub = validate_widget_token(request=request, token=token)
    else:
        token_sub = "test_user"

    commune_instance = None
    if code_commune is not None:
        commune_instance = db_session.get(Commune, code_commune)

    thematiques = list_thematiques(categories) if categories else None
    size = x * y
    results = get_results(
        db_session=db_session,
        page=page,
        code_commune=code_commune,
        publics=[publics],
        thematiques=thematiques,
        score_qualite_minimum=score_qualite_minimum,
        size=size,
    )

    current_url = str(request.url)

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
                "results": results["items"],
                "page": results["page"],
                "pages": results["pages"],
                "x": x,
            },
        )

    commune_label = None
    if commune_instance is not None:
        codes_postaux = commune_instance.codes_postaux
        code_postal = codes_postaux[0] if codes_postaux else None
        commune_label = f"{commune_instance.nom} ({code_postal})"

    return templates.TemplateResponse(
        request=request,
        name="widget.html",
        context={
            "request": request,
            "token": token,
            "token_sub": token_sub,
            "current_url": current_url,
            "schema_categories": v1.Categorie,
            "schema_publics": v1.Public,
            "query_params_commune_code": code_commune,
            "query_params_commune_label": commune_label,
            "query_params_publics": publics,
            "query_params_categories": categories,
            "query_params_x": x,
            "query_params_y": y,
            "query_params_score_qualite_minimum": score_qualite_minimum,
            "query_params_display_form": display_form,
            "query_params_display_communes_filter": display_communes_filter,
            "query_params_display_categories_filter": display_categories_filter,
            "query_params_display_publics_filter": display_publics_filter,
            "results": results["items"],
            "display_form": display_form,
            "display_communes_filter": display_communes_filter,
            "display_categories_filter": display_categories_filter,
            "display_publics_filter": display_publics_filter,
            "page": results["page"],
            "pages": results["pages"],
            "x": x,
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
