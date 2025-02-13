from typing import Annotated, TypeVar

from pydantic.json_schema import SkipJsonSchema

import fastapi

from data_inclusion.api import auth
from data_inclusion.api.config import settings
from data_inclusion.api.core import db
from data_inclusion.api.decoupage_administratif.constants import (
    DepartementCodeEnum,
    DepartementSlugEnum,
    RegionCodeEnum,
    RegionSlugEnum,
)
from data_inclusion.api.decoupage_administratif.models import Commune
from data_inclusion.api.decoupage_administratif.utils import (
    get_departement_by_code_or_slug,
    get_region_by_code_or_slug,
)
from data_inclusion.api.utils import pagination, soliguide
from data_inclusion.api.v0.analytics.services import (
    save_consult_service_event,
    save_consult_structure_event,
    save_list_services_event,
    save_list_structures_event,
    save_search_services_event,
)
from data_inclusion.api.v0.inclusion_data import schemas, services
from data_inclusion.api.v0.inclusion_schema import legacy as di_schema

router = fastapi.APIRouter(tags=["Données"])


# This ensures a dropdown is shown in the openapi doc
# for optional enum query parameters.
T = TypeVar("T")
Optional = T | SkipJsonSchema[None]

CodeCommuneFilter = Annotated[
    Optional[di_schema.CodeCommune],
    fastapi.Query(description="Code insee géographique d'une commune."),
]

CodeDepartementFilter = Annotated[
    Optional[DepartementCodeEnum],
    fastapi.Query(description="Code insee géographique d'un département."),
]

CodeRegionFilter = Annotated[
    Optional[RegionCodeEnum],
    fastapi.Query(description="Code insee géographique d'une région."),
]


@router.get(
    "/structures",
    response_model=pagination.BigPage[schemas.Structure],
    summary="Lister les structures consolidées",
    dependencies=[auth.authenticated_dependency] if settings.TOKEN_ENABLED else [],
)
def list_structures_endpoint(
    request: fastapi.Request,
    background_tasks: fastapi.BackgroundTasks,
    sources: Annotated[
        Optional[list[str]],
        fastapi.Query(
            description="""Une liste d'identifiants de source.
                La liste des identifiants de source est disponible sur le endpoint
                dédié. Les résultats seront limités aux sources spécifiées.
            """,
        ),
    ] = None,
    id: Annotated[Optional[str], fastapi.Query(include_in_schema=False)] = None,
    typologie: Annotated[Optional[di_schema.Typologie], fastapi.Query()] = None,
    label_national: Annotated[
        Optional[di_schema.LabelNational], fastapi.Query()
    ] = None,
    thematiques: Annotated[
        Optional[list[di_schema.Thematique]],
        fastapi.Query(
            description="""Une liste de thématique.
                Chaque résultat renvoyé a (au moins) une thématique dans cette liste."""
        ),
    ] = None,
    code_region: CodeRegionFilter = None,
    slug_region: Annotated[Optional[RegionSlugEnum], fastapi.Query()] = None,
    code_departement: CodeDepartementFilter = None,
    slug_departement: Annotated[Optional[DepartementSlugEnum], fastapi.Query()] = None,
    code_commune: CodeCommuneFilter = None,
    db_session=fastapi.Depends(db.get_session),
):
    region = get_region_by_code_or_slug(code=code_region, slug=slug_region)

    departement = get_departement_by_code_or_slug(
        code=code_departement, slug=slug_departement
    )

    structures_list = services.list_structures(
        request,
        db_session,
        sources=sources,
        typologie=typologie,
        label_national=label_national,
        departement=departement,
        region=region,
        commune_code=code_commune,
        thematiques=thematiques,
    )

    background_tasks.add_task(
        save_list_structures_event,
        request=request,
        db_session=db_session,
        sources=sources,
        typologie=typologie,
        label_national=label_national,
        departement=departement,
        region=region,
        code_commune=code_commune,
        thematiques=thematiques,
    )

    return structures_list


@router.get(
    "/structures/{source}/{id}",
    response_model=schemas.DetailedStructure,
    summary="Détailler une structure",
    dependencies=[auth.authenticated_dependency] if settings.TOKEN_ENABLED else [],
)
def retrieve_structure_endpoint(
    source: Annotated[str, fastapi.Path()],
    id: Annotated[str, fastapi.Path()],
    request: fastapi.Request,
    background_tasks: fastapi.BackgroundTasks,
    db_session=fastapi.Depends(db.get_session),
    _=fastapi.Depends(soliguide.notify_soliguide_dependency),
):
    structure = services.retrieve_structure(
        db_session=db_session,
        source=source,
        id_=id,
    )
    background_tasks.add_task(
        save_consult_structure_event,
        request=request,
        structure=structure,
        db_session=db_session,
    )
    return structure


@router.get(
    "/sources",
    response_model=list[schemas.Source],
    summary="Lister les sources consolidées",
    dependencies=[auth.authenticated_dependency] if settings.TOKEN_ENABLED else [],
)
def list_sources_endpoint(
    request: fastapi.Request,
):
    return services.list_sources(request=request)


@router.get(
    "/services",
    response_model=pagination.BigPage[schemas.Service],
    summary="Lister les services consolidés",
    dependencies=[auth.authenticated_dependency] if settings.TOKEN_ENABLED else [],
)
def list_services_endpoint(
    request: fastapi.Request,
    background_tasks: fastapi.BackgroundTasks,
    db_session=fastapi.Depends(db.get_session),
    sources: Annotated[
        Optional[list[str]],
        fastapi.Query(
            description="""Une liste d'identifiants de source.
                La liste des identifiants de source est disponible sur le endpoint
                dédié. Les résultats seront limités aux sources spécifiées.
            """,
        ),
    ] = None,
    thematiques: Annotated[
        Optional[list[di_schema.Thematique]],
        fastapi.Query(
            description="""Une liste de thématique.
                Chaque résultat renvoyé a (au moins) une thématique dans cette liste."""
        ),
    ] = None,
    code_region: CodeRegionFilter = None,
    slug_region: Annotated[Optional[RegionSlugEnum], fastapi.Query()] = None,
    code_departement: CodeDepartementFilter = None,
    slug_departement: Annotated[Optional[DepartementSlugEnum], fastapi.Query()] = None,
    code_commune: CodeCommuneFilter = None,
    frais: Annotated[
        Optional[list[di_schema.Frais]],
        fastapi.Query(
            description="""Une liste de frais.
                Chaque résultat renvoyé a (au moins) un frais dans cette liste."""
        ),
    ] = None,
    profils: Annotated[
        Optional[list[di_schema.Profil]],
        fastapi.Query(
            description="""Une liste de profils.
                Chaque résultat renvoyé a (au moins) un profil dans cette liste.
            """
        ),
    ] = None,
    recherche_public: Annotated[
        Optional[str],
        fastapi.Query(
            description="""Une recherche en texte intégral parmi toutes
              les valeurs "publics" que nous collectons chez nos producteurs de données.
            """
        ),
    ] = None,
    modes_accueil: Annotated[
        Optional[list[di_schema.ModeAccueil]],
        fastapi.Query(
            description="""Une liste de modes d'accueil.
                Chaque résultat renvoyé a (au moins) un mode d'accueil dans cette liste.
            """
        ),
    ] = None,
    types: Annotated[
        Optional[list[di_schema.TypologieService]],
        fastapi.Query(
            description="""Une liste de typologies de service.
                Chaque résultat renvoyé a (au moins) une typologie dans cette liste."""
        ),
    ] = None,
    score_qualite_minimum: Annotated[
        Optional[float],
        fastapi.Query(
            description="""[BETA] Score de qualité minimum.
                Les résultats renvoyés ont un score de qualité supérieur ou égal à ce
                score."""
        ),
    ] = None,
    inclure_suspendus: Annotated[
        Optional[bool],
        fastapi.Query(
            description="""Inclure les services ayant une date de suspension dépassée.
                Ils sont exclus par défaut.
            """
        ),
    ] = False,
):
    region = get_region_by_code_or_slug(code=code_region, slug=slug_region)
    departement = get_departement_by_code_or_slug(
        code=code_departement, slug=slug_departement
    )

    services_listed = services.list_services(
        request,
        db_session,
        sources=sources,
        thematiques=thematiques,
        departement=departement,
        region=region,
        code_commune=code_commune,
        frais=frais,
        profils=profils,
        recherche_public=recherche_public,
        modes_accueil=modes_accueil,
        types=types,
        score_qualite_minimum=score_qualite_minimum,
        include_outdated=inclure_suspendus,
    )
    background_tasks.add_task(
        save_list_services_event,
        request=request,
        db_session=db_session,
        sources=sources,
        thematiques=thematiques,
        departement=departement,
        region=region,
        code_commune=code_commune,
        frais=frais,
        profils=profils,
        modes_accueil=modes_accueil,
        types=types,
        inclure_suspendus=inclure_suspendus,
    )
    return services_listed


@router.get(
    "/services/{source}/{id}",
    response_model=schemas.DetailedService,
    summary="Détailler un service",
    dependencies=[auth.authenticated_dependency] if settings.TOKEN_ENABLED else [],
)
def retrieve_service_endpoint(
    request: fastapi.Request,
    source: Annotated[str, fastapi.Path()],
    id: Annotated[str, fastapi.Path()],
    background_tasks: fastapi.BackgroundTasks,
    db_session=fastapi.Depends(db.get_session),
    _=fastapi.Depends(soliguide.notify_soliguide_dependency),
):
    service = services.retrieve_service(db_session=db_session, source=source, id_=id)

    background_tasks.add_task(
        save_consult_service_event,
        request=request,
        service=service,
        db_session=db_session,
    )
    return service


@router.get(
    "/search/services",
    response_model=pagination.BigPage[schemas.ServiceSearchResult],
    summary="Rechercher des services",
    dependencies=[auth.authenticated_dependency] if settings.TOKEN_ENABLED else [],
)
def search_services_endpoint(
    request: fastapi.Request,
    background_tasks: fastapi.BackgroundTasks,
    db_session=fastapi.Depends(db.get_session),
    sources: Annotated[
        Optional[list[str]],
        fastapi.Query(
            description="""Une liste d'identifiants de source.
                La liste des identifiants de source est disponible sur le endpoint
                dédié. Les résultats seront limités aux sources spécifiées.
            """,
        ),
    ] = None,
    code_commune: Annotated[
        Optional[di_schema.CodeCommune],
        fastapi.Query(
            description="""Code insee de la commune considérée.
                Si fourni, les résultats inclus également les services proches de
                cette commune. Les résultats sont triés par ordre de distance
                croissante.
            """
        ),
    ] = None,
    code_insee: Annotated[
        Optional[di_schema.CodeCommune],
        fastapi.Query(include_in_schema=False),
    ] = None,
    lat: Annotated[
        Optional[float],
        fastapi.Query(
            description="""Latitude du point de recherche.
                Nécessite également de fournir `lon`.
                Les résultats sont triés par ordre de distance croissante à ce point.
            """
        ),
    ] = None,
    lon: Annotated[
        Optional[float],
        fastapi.Query(
            description="""Longitude du point de recherche.
                Nécessite également de fournir `lat`.
                Les résultats sont triés par ordre de distance croissante à ce point.
            """
        ),
    ] = None,
    thematiques: Annotated[
        Optional[list[di_schema.Thematique]],
        fastapi.Query(
            description="""Une liste de thématique.
                Chaque résultat renvoyé a (au moins) une thématique dans cette liste."""
        ),
    ] = None,
    frais: Annotated[
        Optional[list[di_schema.Frais]],
        fastapi.Query(
            description="""Une liste de frais.
                Chaque résultat renvoyé a (au moins) un frais dans cette liste."""
        ),
    ] = None,
    modes_accueil: Annotated[
        Optional[list[di_schema.ModeAccueil]],
        fastapi.Query(
            description="""Une liste de modes d'accueil.
                Chaque résultat renvoyé a (au moins) un mode d'accueil dans cette liste.
            """
        ),
    ] = None,
    profils: Annotated[
        Optional[list[di_schema.Profil]],
        fastapi.Query(
            description="""Une liste de profils.
                Chaque résultat renvoyé a (au moins) un profil dans cette liste.
            """
        ),
    ] = None,
    recherche_public: Annotated[
        Optional[str],
        fastapi.Query(
            description="""Une recherche en texte intégral parmi toutes
              les valeurs "publics" que nous collectons chez nos producteurs de données.
            """
        ),
    ] = None,
    types: Annotated[
        Optional[list[di_schema.TypologieService]],
        fastapi.Query(
            description="""Une liste de typologies de service.
                Chaque résultat renvoyé a (au moins) une typologie dans cette liste."""
        ),
    ] = None,
    score_qualite_minimum: Annotated[
        Optional[float],
        fastapi.Query(
            description="""Score de qualité minimum.
                Les résultats renvoyés ont un score de qualité supérieur ou égal à ce
                score."""
        ),
    ] = None,
    inclure_suspendus: Annotated[
        Optional[bool],
        fastapi.Query(
            description="""Inclure les services ayant une date de suspension dépassée.
                Ils sont exclus par défaut.
            """
        ),
    ] = False,
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

    if code_commune is None and code_insee is not None:
        code_commune = code_insee

    commune_instance = None
    search_point = None
    if code_commune is not None:
        commune_instance = db_session.get(Commune, code_commune)
        if commune_instance is None:
            raise fastapi.HTTPException(
                status_code=fastapi.status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="This `code_commune` does not exist.",
            )
        if lat and lon:
            search_point = f"POINT({lon} {lat})"
        elif lat or lon:
            raise fastapi.HTTPException(
                status_code=fastapi.status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="The `lat` and `lon` must be simultaneously filled.",
            )

    results = services.search_services(
        request,
        db_session,
        sources=sources,
        commune_instance=commune_instance,
        thematiques=thematiques,
        frais=frais,
        modes_accueil=modes_accueil,
        profils=profils,
        profils_search=recherche_public,
        types=types,
        search_point=search_point,
        score_qualite_minimum=score_qualite_minimum,
        include_outdated=inclure_suspendus,
    )

    background_tasks.add_task(
        save_search_services_event,
        request=request,
        db_session=db_session,
        results=results,
        sources=sources,
        code_commune=code_commune,
        lat=lat,
        lon=lon,
        thematiques=thematiques,
        frais=frais,
        modes_accueil=modes_accueil,
        profils=profils,
        types=types,
        inclure_suspendus=inclure_suspendus,
    )

    return results
