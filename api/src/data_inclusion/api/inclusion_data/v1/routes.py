from typing import Annotated

import fastapi

from data_inclusion.api import auth
from data_inclusion.api.analytics.v1.services import save_event
from data_inclusion.api.config import settings
from data_inclusion.api.core import db
from data_inclusion.api.decoupage_administratif.models import Commune
from data_inclusion.api.inclusion_data.v1 import parameters, schemas, services
from data_inclusion.api.utils import pagination, soliguide

router = fastapi.APIRouter(tags=["v1 | Données"])


@router.get(
    "/structures",
    response_model=pagination.Page[schemas.ListedStructure],
    summary="Lister les structures consolidées",
    dependencies=[auth.authenticated_dependency] if settings.TOKEN_ENABLED else [],
)
def list_structures_endpoint(
    request: fastapi.Request,
    background_tasks: fastapi.BackgroundTasks,
    params: Annotated[parameters.ListStructuresQueryParams, fastapi.Query()],
    db_session=fastapi.Depends(db.get_session),
):
    page = pagination.paginate(
        db_session=db_session,
        query=services.list_structures_query(
            params=params,
            include_soliguide=soliguide.is_allowed_user(request),
        ),
        size=params.size,
        page=params.page,
    )
    background_tasks.add_task(
        save_event,
        request=request,
        params=params,
        db_session=db_session,
    )
    return page


@router.get(
    "/structures/{id}",
    response_model=schemas.DetailedStructure,
    summary="Détailler une structure",
    dependencies=[auth.authenticated_dependency] if settings.TOKEN_ENABLED else [],
)
def retrieve_structure_endpoint(
    request: fastapi.Request,
    background_tasks: fastapi.BackgroundTasks,
    params: Annotated[parameters.RetrieveStructurePathParams, fastapi.Path()],
    db_session=fastapi.Depends(db.get_session),
    _=fastapi.Depends(soliguide.notify_soliguide_dependency),
):
    structure = services.retrieve_structure(
        db_session=db_session,
        params=params,
    )
    if structure is None:
        raise fastapi.HTTPException(status_code=404)
    background_tasks.add_task(
        save_event,
        request=request,
        params=params,
        db_session=db_session,
    )
    return structure


@router.get(
    "/sources",
    response_model=list[schemas.Source],
    summary="Lister les sources consolidées",
    dependencies=[auth.authenticated_dependency] if settings.TOKEN_ENABLED else [],
)
def list_sources_endpoint():
    return services.list_sources()


@router.get(
    "/services",
    response_model=pagination.Page[schemas.Service],
    summary="Lister les services consolidés",
    dependencies=[auth.authenticated_dependency] if settings.TOKEN_ENABLED else [],
)
def list_services_endpoint(
    request: fastapi.Request,
    background_tasks: fastapi.BackgroundTasks,
    params: Annotated[parameters.ListServicesQueryParams, fastapi.Query()],
    db_session=fastapi.Depends(db.get_session),
):
    page = pagination.paginate(
        db_session=db_session,
        query=services.list_services_query(
            params=params,
            include_soliguide=soliguide.is_allowed_user(request),
        ),
        size=params.size,
        page=params.page,
    )
    background_tasks.add_task(
        save_event,
        request=request,
        params=params,
        db_session=db_session,
    )

    is_extra_visible = request.query_params.get("extra", "false").lower() == "true"

    # manually serialize to pass context for extra field visibility
    return pagination.Page[schemas.Service].model_validate(
        page,
        context={"is_extra_visible": is_extra_visible},
    )


@router.get(
    "/services/{id}",
    response_model=schemas.DetailedService,
    summary="Détailler un service",
    dependencies=[auth.authenticated_dependency] if settings.TOKEN_ENABLED else [],
)
def retrieve_service_endpoint(
    request: fastapi.Request,
    params: Annotated[parameters.RetrieveServicePathParams, fastapi.Path()],
    background_tasks: fastapi.BackgroundTasks,
    db_session=fastapi.Depends(db.get_session),
    _=fastapi.Depends(soliguide.notify_soliguide_dependency),
):
    service = services.retrieve_service(
        db_session=db_session,
        params=params,
    )
    if service is None:
        raise fastapi.HTTPException(status_code=404)
    background_tasks.add_task(
        save_event,
        request=request,
        params=params,
        db_session=db_session,
        score_qualite=service.score_qualite,
    )
    return service


@router.get(
    "/search/services",
    response_model=pagination.Page[schemas.ServiceSearchResult],
    summary="Rechercher des services",
    dependencies=[auth.authenticated_dependency] if settings.TOKEN_ENABLED else [],
)
def search_services_endpoint(
    request: fastapi.Request,
    background_tasks: fastapi.BackgroundTasks,
    params: Annotated[parameters.SearchServicesQueryParams, fastapi.Query()],
    db_session=fastapi.Depends(db.get_session),
):
    """
    ## Rechercher des services

    La recherche de services permet de trouver des services dans une commune et à
    proximité.

    Les services peuvent être filtrés selon par thématiques, frais, typologies et
    code_insee de commune.

    En particulier, lorsqu'un `code_commune` est fourni :

    * les services sont filtrés par zone de diffusion lorsque celle-ci est définie.
    * de plus, les services en présentiel sont filtrés dans un rayon de 50km autour de
    la commune ou du point de recherche fourni.
    * le champ `distance` est :
        * rempli pour les services (non exclusivement) en présentiel.
        * laissé vide pour les services à distance et par défaut si le mode d'accueil
        n'est pas définie.
    * les résultats sont triés par distance croissante.
    """

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
        commune_instance=commune_instance,
        include_soliguide=soliguide.is_allowed_user(request),
    )

    page = pagination.paginate(
        db_session=db_session,
        query=query,
        size=params.size,
        page=params.page,
        mapping=mapping,
    )

    background_tasks.add_task(
        save_event,
        request=request,
        params=params,
        db_session=db_session,
        first_results_page=page,
    )

    return page
