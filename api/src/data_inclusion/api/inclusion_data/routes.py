from typing import Annotated, TypeVar

from pydantic.json_schema import SkipJsonSchema

import fastapi

from data_inclusion import schema as di_schema
from data_inclusion.api import auth
from data_inclusion.api.code_officiel_geo.constants import (
    CODE_COMMUNE_BY_CODE_ARRONDISSEMENT,
    DepartementCOG,
    DepartementSlug,
)
from data_inclusion.api.code_officiel_geo.models import Commune
from data_inclusion.api.config import settings
from data_inclusion.api.core import db
from data_inclusion.api.inclusion_data import schemas, services
from data_inclusion.api.utils import pagination, soliguide

router = fastapi.APIRouter(tags=["Données"])


# This ensures a dropdown is shown in the openapi doc
# for optional enum query parameters.
T = TypeVar("T")
Optional = T | SkipJsonSchema[None]


@router.get(
    "/structures",
    response_model=pagination.BigPage[schemas.Structure],
    summary="Lister les structures consolidées",
    dependencies=[auth.authenticated_dependency] if settings.TOKEN_ENABLED else [],
)
def list_structures_endpoint(
    request: fastapi.Request,
    source: Annotated[Optional[str], fastapi.Query()] = None,
    id: Annotated[Optional[str], fastapi.Query()] = None,
    typologie: Annotated[Optional[di_schema.Typologie], fastapi.Query()] = None,
    label_national: Annotated[
        Optional[di_schema.LabelNational], fastapi.Query()
    ] = None,
    thematique: Annotated[Optional[di_schema.Thematique], fastapi.Query()] = None,
    departement: Annotated[Optional[DepartementCOG], fastapi.Query()] = None,
    departement_slug: Annotated[Optional[DepartementSlug], fastapi.Query()] = None,
    code_postal: Annotated[Optional[di_schema.CodePostal], fastapi.Query()] = None,
    db_session=fastapi.Depends(db.get_session),
):
    return services.list_structures(
        request,
        db_session,
        source=source,
        id_=id,
        typologie=typologie,
        label_national=label_national,
        departement=departement,
        departement_slug=departement_slug,
        code_postal=code_postal,
        thematique=thematique,
    )


@router.get(
    "/structures/{source}/{id}",
    response_model=schemas.DetailedStructure,
    summary="Détailler une structure",
    dependencies=[auth.authenticated_dependency] if settings.TOKEN_ENABLED else [],
)
def retrieve_structure_endpoint(
    source: Annotated[str, fastapi.Path()],
    id: Annotated[str, fastapi.Path()],
    db_session=fastapi.Depends(db.get_session),
    _=fastapi.Depends(soliguide.notify_soliguide_dependency),
):
    return services.retrieve_structure(db_session=db_session, source=source, id_=id)


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
    db_session=fastapi.Depends(db.get_session),
    source: Annotated[Optional[str], fastapi.Query()] = None,
    thematique: Annotated[
        Optional[di_schema.Thematique],
        fastapi.Query(
            deprecated=True,
            description="Déprécié en faveur de `thematiques`.",
        ),
    ] = None,
    thematiques: Annotated[
        Optional[list[di_schema.Thematique]],
        fastapi.Query(
            description="""Une liste de thématique.
                Chaque résultat renvoyé a (au moins) une thématique dans cette liste."""
        ),
    ] = None,
    departement: Annotated[Optional[DepartementCOG], fastapi.Query()] = None,
    departement_slug: Annotated[Optional[DepartementSlug], fastapi.Query()] = None,
    code_insee: Annotated[Optional[di_schema.CodeCommune], fastapi.Query()] = None,
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
    inclure_suspendus: Annotated[
        Optional[bool],
        fastapi.Query(
            description="""Inclure les services ayant une date de suspension dépassée.
                Ils sont exclus par défaut.
            """
        ),
    ] = False,
):
    if thematiques is None and thematique is not None:
        thematiques = [thematique]

    return services.list_services(
        request,
        db_session,
        source=source,
        thematiques=thematiques,
        departement=departement,
        departement_slug=departement_slug,
        code_insee=code_insee,
        frais=frais,
        profils=profils,
        modes_accueil=modes_accueil,
        types=types,
        include_outdated=inclure_suspendus,
    )


@router.get(
    "/services/{source}/{id}",
    response_model=schemas.DetailedService,
    summary="Détailler un service",
    dependencies=[auth.authenticated_dependency] if settings.TOKEN_ENABLED else [],
)
def retrieve_service_endpoint(
    source: Annotated[str, fastapi.Path()],
    id: Annotated[str, fastapi.Path()],
    db_session=fastapi.Depends(db.get_session),
    _=fastapi.Depends(soliguide.notify_soliguide_dependency),
):
    return services.retrieve_service(db_session=db_session, source=source, id_=id)


@router.get(
    "/search/services",
    response_model=pagination.BigPage[schemas.ServiceSearchResult],
    summary="Rechercher des services",
    dependencies=[auth.authenticated_dependency] if settings.TOKEN_ENABLED else [],
)
def search_services_endpoint(
    request: fastapi.Request,
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
    code_insee: Annotated[
        Optional[di_schema.CodeCommune],
        fastapi.Query(
            description="""Code insee de la commune considérée.
                Si fourni, les résultats inclus également les services proches de
                cette commune. Les résultats sont triés par ordre de distance
                croissante.
            """
        ),
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
    types: Annotated[
        Optional[list[di_schema.TypologieService]],
        fastapi.Query(
            description="""Une liste de typologies de service.
                Chaque résultat renvoyé a (au moins) une typologie dans cette liste."""
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

    En particulier, lorsque le `code_insee` d'une commune est fourni :

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
    search_point = None
    if code_insee is not None:
        code_insee = CODE_COMMUNE_BY_CODE_ARRONDISSEMENT.get(code_insee, code_insee)
        commune_instance = db_session.get(Commune, code_insee)
        if commune_instance is None:
            raise fastapi.HTTPException(
                status_code=fastapi.status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="This `code_insee` does not exist.",
            )
        if lat and lon:
            search_point = f"POINT({lon} {lat})"
        elif lat or lon:
            raise fastapi.HTTPException(
                status_code=fastapi.status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="The `lat` and `lon` must be simultaneously filled.",
            )

    return services.search_services(
        request,
        db_session,
        sources=sources,
        commune_instance=commune_instance,
        thematiques=thematiques,
        frais=frais,
        modes_accueil=modes_accueil,
        profils=profils,
        types=types,
        search_point=search_point,
        include_outdated=inclure_suspendus,
    )
