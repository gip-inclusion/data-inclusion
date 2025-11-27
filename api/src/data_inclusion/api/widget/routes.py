from pathlib import Path
from typing import Annotated

import pydantic

import fastapi
from fastapi import staticfiles, templating

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


def get_results(
    db_session,
    page: int,
    code_commune: str | None,
    publics: v1.Public | None,
    thematiques: list[v1.Thematique] | None = None,
):
    params = parameters.SearchServicesQueryParams(
        code_commune=code_commune,
        thematiques=thematiques,
        publics=[publics] if publics is not None else None,
        score_qualite_minimum=0.8,
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
        size=9,
        page=page,
        mapping=mapping,
    )


def list_thematiques(category: v1.Categorie) -> list[v1.Thematique]:
    return [t for t in v1.Thematique if t.value.startswith(category.value)]


@app.get(path="/")
def widget(
    request: fastapi.Request,
    code_commune: str | None = None,
    categories: v1.Categorie | None = None,
    publics: v1.Public | None = None,
    hx_request: Annotated[bool, fastapi.Header()] = False,
    page: Annotated[int, pydantic.Field(ge=1)] = 1,
    db_session=fastapi.Depends(db.get_session),
) -> fastapi.responses.HTMLResponse:
    if code_commune is not None:
        code_commune = "59350"

    results = get_results(
        db_session=db_session,
        page=page,
        code_commune=code_commune,
        publics=publics,
        thematiques=list_thematiques(categories) if categories is not None else None,
    )

    if hx_request:
        return templates.TemplateResponse(
            request=request,
            name="fragments/results.html",
            context={
                "request": request,
                "results": results["items"],
                "page": results["page"],
                "pages": results["pages"],
            },
        )

    return templates.TemplateResponse(
        request=request,
        name="widget.html",
        context={
            "request": request,
            "schema_categories": v1.Categorie,
            "schema_publics": v1.Public,
            "query_params_commune": code_commune,
            "query_params_publics": publics,
            "query_params_categories": categories,
            "results": results["items"],
            "page": results["page"],
            "pages": results["pages"],
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
