import functools
import json
import logging
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Annotated, Optional

import geoalchemy2
import sentry_sdk
import sqlalchemy as sqla
from pydantic.json_schema import SkipJsonSchema
from sqlalchemy import orm

import fastapi
import fastapi_pagination
from fastapi import middleware
from fastapi.middleware import cors
from fastapi.security import HTTPBearer
from fastapi_pagination.ext.sqlalchemy import paginate

from data_inclusion import schema as di_schema
from data_inclusion.api import models, schemas, settings
from data_inclusion.api.core import auth, db, jwt
from data_inclusion.api.core.request.middleware import save_request_middleware
from data_inclusion.api.doc_api.router import router as doc_api_router
from data_inclusion.api.utils import code_officiel_geographique, pagination, soliguide

logger = logging.getLogger(__name__)


@functools.cache
def get_thematiques_by_group():
    thematiques = defaultdict(list)
    for thematique in di_schema.Thematique:
        try:
            theme, _ = str(thematique.value).split("--")
        except ValueError:
            continue
        thematiques[theme].append(thematique.value)
    return thematiques


def get_sub_thematiques(thematiques: list[di_schema.Thematique]) -> list[str]:
    """
    get_sub_thematiques(Thematique.MOBILITE) -> [
        "mobilite",
        "mobilite--accompagnement-a-la-mobilite"
        ...
    ]

    get_sub_thematiques(Thematique.MOBILITE_ACCOMPAGNEMENT_A_LA_MOBILITE") -> [
        "mobilite--accompagnement-a-la-mobilite"
    ]
    """
    all_thematiques = set()
    for t in thematiques:
        all_thematiques.add(t.value)
        group = get_thematiques_by_group().get(t.value)
        if group:
            all_thematiques.update(group)
    return list(all_thematiques)


description = (Path(__file__).parent / "api_description.md").read_text()


def create_app() -> fastapi.FastAPI:
    # sentry must be initialized before app
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        traces_sample_rate=1.0,
        enable_tracing=True,
        environment=settings.ENV,
    )

    app = fastapi.FastAPI(
        title="data.inclusion API",
        openapi_url="/api/openapi.json",
        description=description,
        docs_url="/api/v0/docs",
        contact={
            "name": "data.inclusion",
            "email": "data.inclusion@beta.gouv.fr",
            "url": "https://www.data.inclusion.beta.gouv.fr/",
        },
        middleware=[
            middleware.Middleware(
                cors.CORSMiddleware,
                allow_origins=settings.CORS_ALLOWED_ORIGINS,
                allow_credentials=True,
                allow_methods=["*"],
                allow_headers=["*"],
            ),
            middleware.Middleware(
                auth.AuthenticationMiddleware,
                backend=auth.AuthenticationBackend(),
                on_error=auth.on_error,
            ),
        ],
        debug=settings.DEBUG,
    )

    if settings.ENV == "dev":
        from debug_toolbar.middleware import DebugToolbarMiddleware

        app.add_middleware(
            DebugToolbarMiddleware,
            panels=["debug_toolbar.panels.sqlalchemy.SQLAlchemyPanel"],
        )

    app.middleware("http")(db.db_session_middleware)
    app.middleware("http")(save_request_middleware)

    app.include_router(v0_api_router)
    app.include_router(doc_api_router, prefix="/api/v0/doc", tags=["Documentation"])

    fastapi_pagination.add_pagination(app)

    return app


v0_api_router = fastapi.APIRouter(
    prefix="/api/v0",
    dependencies=[fastapi.Depends(HTTPBearer())] if settings.TOKEN_ENABLED else [],
    tags=["Données"],
)


def list_structures(
    request: fastapi.Request,
    db_session: orm.Session,
    source: Optional[str] = None,
    id_: Optional[str] = None,
    typologie: Optional[di_schema.Typologie] = None,
    label_national: Optional[di_schema.LabelNational] = None,
    departement: Optional[schemas.DepartementCOG] = None,
    departement_slug: Optional[schemas.DepartementSlug] = None,
    code_postal: Optional[di_schema.CodePostal] = None,
    thematique: Optional[di_schema.Thematique] = None,
) -> list:
    query = sqla.select(models.Structure)

    if source is not None:
        query = query.filter_by(source=source)

    if not request.user.is_authenticated or "dora" not in request.user.username:
        query = query.filter(models.Structure.source != "soliguide")
        query = query.filter(models.Structure.source != "data-inclusion")

    if id_ is not None:
        query = query.filter_by(id=id_)

    if departement is not None:
        query = query.filter(
            sqla.or_(
                models.Structure.code_insee.startswith(departement.value),
                models.Structure._di_geocodage_code_insee.startswith(departement.value),
            )
        )

    if departement_slug is not None:
        query = query.filter(
            sqla.or_(
                models.Structure.code_insee.startswith(
                    schemas.DepartementCOG[departement_slug.name].value
                ),
                models.Structure._di_geocodage_code_insee.startswith(
                    schemas.DepartementCOG[departement_slug.name].value
                ),
            )
        )

    if code_postal is not None:
        query = query.filter_by(code_postal=code_postal)

    if typologie is not None:
        query = query.filter_by(typologie=typologie.value)

    if label_national is not None:
        query = query.filter(
            models.Structure.labels_nationaux.contains([label_national.value])
        )

    if thematique is not None:
        filter_stmt = """\
        EXISTS(
            SELECT
            FROM unnest(thematiques) thematique
            WHERE thematique ~ ('^' || :thematique)
        )
        """
        query = query.filter(
            sqla.text(filter_stmt).bindparams(thematique=thematique.value)
        )

    query = query.order_by(
        models.Structure.source,
        models.Structure.id,
    )

    return paginate(db_session, query)


@v0_api_router.get(
    "/structures",
    response_model=pagination.Page[schemas.Structure],
    summary="Lister les structures consolidées",
)
def list_structures_endpoint(
    request: fastapi.Request,
    source: Annotated[str | SkipJsonSchema[None], fastapi.Query()] = None,
    id: Annotated[str | SkipJsonSchema[None], fastapi.Query()] = None,
    typologie: Annotated[
        di_schema.Typologie | SkipJsonSchema[None], fastapi.Query()
    ] = None,
    label_national: Annotated[
        di_schema.LabelNational | SkipJsonSchema[None], fastapi.Query()
    ] = None,
    thematique: Annotated[
        di_schema.Thematique | SkipJsonSchema[None], fastapi.Query()
    ] = None,
    departement: Annotated[
        schemas.DepartementCOG | SkipJsonSchema[None], fastapi.Query()
    ] = None,
    departement_slug: Annotated[
        schemas.DepartementSlug | SkipJsonSchema[None], fastapi.Query()
    ] = None,
    code_postal: Annotated[
        di_schema.CodePostal | SkipJsonSchema[None], fastapi.Query()
    ] = None,
    db_session=fastapi.Depends(db.get_session),
):
    """
    ## Lister les structures consolidées par data.inclusion

    Il s'agit du point d'entrée principal de l'API, permettant d'accéder finement au
    données structures publiées régulièrement en open data sur data.gouv.

    ### Filtres disponibles

    Les structures peuvent être filtrées par thematique, typologie, label, source, id,
    etc.

    ### Identification des structures

    Les structures sont identifiées de manière unique par le couple :
    * `source` : slug précisant la source de manière unique
    * `id` : l'identifiant local dans la source

    """

    return list_structures(
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


@v0_api_router.get(
    "/structures/{source}/{id}",
    response_model=schemas.DetailedStructure,
    summary="Détailler une structure",
)
def retrieve_structure_endpoint(
    source: Annotated[str, fastapi.Path()],
    id: Annotated[str, fastapi.Path()],
    db_session=fastapi.Depends(db.get_session),
    _=fastapi.Depends(soliguide.notify_soliguide_dependency),
):
    structure_instance = db_session.scalars(
        sqla.select(models.Structure)
        .options(orm.selectinload(models.Structure.services))
        .filter_by(source=source)
        .filter_by(id=id)
    ).first()

    if structure_instance is None:
        raise fastapi.HTTPException(status_code=404)

    return structure_instance


@functools.cache
def read_sources():
    return json.loads((Path(__file__).parent / "sources.json").read_text())


def list_sources(request: fastapi.Request) -> list[dict]:
    sources = read_sources()
    if not request.user.is_authenticated or "dora" not in request.user.username:
        sources = [
            d for d in sources if d["slug"] not in ["data-inclusion", "soliguide"]
        ]
    return sources


@v0_api_router.get(
    "/sources",
    response_model=list[schemas.Source],
    summary="Lister les sources consolidées",
)
def list_sources_endpoint(
    request: fastapi.Request,
):
    return list_sources(request=request)


def list_services(
    request: fastapi.Request,
    db_session: orm.Session,
    source: Optional[str] = None,
    thematique: Optional[di_schema.Thematique] = None,
    departement: Optional[schemas.DepartementCOG] = None,
    departement_slug: Optional[schemas.DepartementSlug] = None,
    code_insee: Optional[di_schema.CodeCommune] = None,
):
    query = (
        sqla.select(models.Service)
        .join(models.Service.structure)
        .options(orm.contains_eager(models.Service.structure))
    )

    if source is not None:
        query = query.filter(models.Structure.source == source)

    if not request.user.is_authenticated or "dora" not in request.user.username:
        query = query.filter(models.Structure.source != "soliguide")
        query = query.filter(models.Structure.source != "data-inclusion")

    if departement is not None:
        query = query.filter(
            sqla.or_(
                models.Service.code_insee.startswith(departement.value),
                models.Service._di_geocodage_code_insee.startswith(departement.value),
            )
        )

    if departement_slug is not None:
        query = query.filter(
            sqla.or_(
                models.Service.code_insee.startswith(
                    schemas.DepartementCOG[departement_slug.name].value
                ),
                models.Service._di_geocodage_code_insee.startswith(
                    schemas.DepartementCOG[departement_slug.name].value
                ),
            )
        )

    if code_insee is not None:
        code_insee = code_officiel_geographique.CODE_COMMUNE_BY_CODE_ARRONDISSEMENT.get(
            code_insee, code_insee
        )

        query = query.filter(
            sqla.or_(
                models.Service.code_insee == code_insee,
                models.Service._di_geocodage_code_insee == code_insee,
            )
        )

    if thematique is not None:
        filter_stmt = """\
        EXISTS(
            SELECT
            FROM unnest(service.thematiques) thematique
            WHERE thematique ~ ('^' || :thematique)
        )
        """
        query = query.filter(
            sqla.text(filter_stmt).bindparams(thematique=thematique.value)
        )

    query = query.order_by(
        models.Service.source,
        models.Service.id,
    )

    return paginate(db_session, query, unique=False)


@v0_api_router.get(
    "/services",
    response_model=pagination.Page[schemas.Service],
    summary="Lister les services consolidées",
)
def list_services_endpoint(
    request: fastapi.Request,
    db_session=fastapi.Depends(db.get_session),
    source: Annotated[str | SkipJsonSchema[None], fastapi.Query()] = None,
    thematique: Annotated[
        di_schema.Thematique | SkipJsonSchema[None], fastapi.Query()
    ] = None,
    departement: Annotated[
        schemas.DepartementCOG | SkipJsonSchema[None], fastapi.Query()
    ] = None,
    departement_slug: Annotated[
        schemas.DepartementSlug | SkipJsonSchema[None], fastapi.Query()
    ] = None,
    code_insee: Annotated[
        di_schema.CodeCommune | SkipJsonSchema[None], fastapi.Query()
    ] = None,
):
    return list_services(
        request,
        db_session,
        source=source,
        thematique=thematique,
        departement=departement,
        departement_slug=departement_slug,
        code_insee=code_insee,
    )


@v0_api_router.get(
    "/services/{source}/{id}",
    response_model=schemas.DetailedService,
    summary="Détailler un service",
)
def retrieve_service_endpoint(
    source: Annotated[str, fastapi.Path()],
    id: Annotated[str, fastapi.Path()],
    db_session=fastapi.Depends(db.get_session),
    _=fastapi.Depends(soliguide.notify_soliguide_dependency),
):
    service_instance = db_session.scalars(
        sqla.select(models.Service)
        .options(orm.selectinload(models.Service.structure))
        .filter_by(source=source)
        .filter_by(id=id)
    ).first()

    if service_instance is None:
        raise fastapi.HTTPException(status_code=404)

    return service_instance


def search_services(
    request: fastapi.Request,
    db_session: orm.Session,
    sources: Optional[list[str]] = None,
    commune_instance: Optional[models.Commune] = None,
    thematiques: Optional[list[di_schema.Thematique]] = None,
    frais: Optional[list[di_schema.Frais]] = None,
    types: Optional[list[di_schema.TypologieService]] = None,
    search_point: Optional[str] = None,
    include_outdated: Optional[bool] = False,
):
    query = (
        sqla.select(models.Service)
        .join(models.Service.structure)
        .options(orm.contains_eager(models.Service.structure))
    )

    if sources is not None:
        query = query.filter(models.Service.source == sqla.any_(sqla.literal(sources)))

    if not request.user.is_authenticated or "dora" not in request.user.username:
        query = query.filter(models.Structure.source != "soliguide")
        query = query.filter(models.Structure.source != "data-inclusion")

    if commune_instance is not None:
        # filter by zone de diffusion
        query = query.filter(
            sqla.or_(
                models.Service.zone_diffusion_type.is_(None),
                models.Service.zone_diffusion_type
                == di_schema.ZoneDiffusionType.PAYS.value,
                sqla.and_(
                    models.Service.zone_diffusion_type
                    == di_schema.ZoneDiffusionType.COMMUNE.value,
                    models.Service.zone_diffusion_code == commune_instance.code,
                ),
                sqla.and_(
                    models.Service.zone_diffusion_type
                    == di_schema.ZoneDiffusionType.EPCI.value,
                    sqla.literal(commune_instance.siren_epci).contains(
                        models.Service.zone_diffusion_code
                    ),
                ),
                sqla.and_(
                    models.Service.zone_diffusion_type
                    == di_schema.ZoneDiffusionType.DEPARTEMENT.value,
                    models.Service.zone_diffusion_code == commune_instance.departement,
                ),
                sqla.and_(
                    models.Service.zone_diffusion_type
                    == di_schema.ZoneDiffusionType.REGION.value,
                    models.Service.zone_diffusion_code == commune_instance.region,
                ),
            )
        )

        src_geometry = sqla.cast(
            geoalchemy2.functions.ST_MakePoint(
                models.Service.longitude, models.Service.latitude
            ),
            geoalchemy2.Geography(geometry_type="GEOMETRY", srid=4326),
        )

        if search_point is not None:
            dest_geometry = search_point
        else:
            dest_geometry = (
                sqla.select(
                    sqla.cast(
                        geoalchemy2.functions.ST_Simplify(models.Commune.geom, 0.01),
                        geoalchemy2.Geography(geometry_type="GEOMETRY", srid=4326),
                    )
                )
                .filter(models.Commune.code == commune_instance.code)
                .scalar_subquery()
            )

        query = query.filter(
            sqla.or_(
                # either `en-presentiel` within a given distance
                geoalchemy2.functions.ST_DWithin(
                    src_geometry,
                    dest_geometry,
                    50_000,  # meters or 50km
                ),
                # or `a-distance`
                models.Service.modes_accueil.contains(
                    sqla.literal([di_schema.ModeAccueil.A_DISTANCE.value])
                ),
            )
        )

        # annotate distance
        query = query.add_columns(
            (
                sqla.case(
                    (
                        models.Service.modes_accueil.contains(
                            sqla.literal([di_schema.ModeAccueil.EN_PRESENTIEL.value])
                        ),
                        (
                            geoalchemy2.functions.ST_Distance(
                                src_geometry,
                                dest_geometry,
                            )
                            / 1000
                        ).cast(sqla.Integer),  # conversion to kms
                    ),
                    else_=sqla.null().cast(sqla.Integer),
                )
            ).label("distance")
        )

    else:
        query = query.add_columns(sqla.null().cast(sqla.Integer).label("distance"))

    if thematiques is not None:
        query = query.filter(
            sqla.text("service.thematiques && :thematiques").bindparams(
                thematiques=get_sub_thematiques(thematiques),
            )
        )

    if frais is not None:
        filter_stmt = """\
        EXISTS(
            SELECT
            FROM unnest(service.frais) frais
            WHERE frais = ANY(:frais)
        )
        """
        query = query.filter(
            sqla.text(filter_stmt).bindparams(frais=[f.value for f in frais])
        )

    if types is not None:
        filter_stmt = """\
        EXISTS(
            SELECT
            FROM unnest(service.types) types
            WHERE types = ANY(:types)
        )
        """
        query = query.filter(
            sqla.text(filter_stmt).bindparams(types=[t.value for t in types])
        )

    if not include_outdated:
        query = query.filter(
            sqla.or_(
                models.Service.date_suspension.is_(None),
                models.Service.date_suspension >= date.today(),
            )
        )

    query = query.order_by(sqla.column("distance").nulls_last())

    def _items_to_mappings(items: list) -> list[dict]:
        # convert rows returned by `Session.execute` to a list of dicts that will be
        # used to instanciate pydantic models
        return [{"service": item[0], "distance": item[1]} for item in items]

    return paginate(db_session, query, unique=False, transformer=_items_to_mappings)


@v0_api_router.get(
    "/search/services",
    response_model=pagination.Page[schemas.ServiceSearchResult],
    summary="Rechercher des services",
)
def search_services_endpoint(
    request: fastapi.Request,
    db_session=fastapi.Depends(db.get_session),
    source: Annotated[
        str | SkipJsonSchema[None],
        fastapi.Query(
            description="""Un identifiant de source.
                Déprécié en faveur de `sources`.
            """,
            deprecated=True,
        ),
    ] = None,
    sources: Annotated[
        list[str] | SkipJsonSchema[None],
        fastapi.Query(
            description="""Une liste d'identifiants de source.
                La liste des identifiants de source est disponible sur le endpoint
                dédié. Les résultats seront limités aux sources spécifiées.
            """,
        ),
    ] = None,
    code_insee: Annotated[
        di_schema.CodeCommune | SkipJsonSchema[None],
        fastapi.Query(
            description="""Code insee de la commune considérée.
                Si fourni, les résultats inclus également les services proches de
                cette commune. Les résultats sont triés par ordre de distance
                croissante.
            """
        ),
    ] = None,
    lat: Annotated[
        float | SkipJsonSchema[None],
        fastapi.Query(
            description="""Latitude du point de recherche.
                Nécessite également de fournir `lon`.
                Les résultats sont triés par ordre de distance croissante à ce point.
            """
        ),
    ] = None,
    lon: Annotated[
        float | SkipJsonSchema[None],
        fastapi.Query(
            description="""Longitude du point de recherche.
                Nécessite également de fournir `lat`.
                Les résultats sont triés par ordre de distance croissante à ce point.
            """
        ),
    ] = None,
    thematiques: Annotated[
        list[di_schema.Thematique] | SkipJsonSchema[None],
        fastapi.Query(
            description="""Une liste de thématique.
                Chaque résultat renvoyé a (au moins) une thématique dans cette liste."""
        ),
    ] = None,
    frais: Annotated[
        list[di_schema.Frais] | SkipJsonSchema[None],
        fastapi.Query(
            description="""Une liste de frais.
                Chaque résultat renvoyé a (au moins) un frais dans cette liste."""
        ),
    ] = None,
    types: Annotated[
        list[di_schema.TypologieService] | SkipJsonSchema[None],
        fastapi.Query(
            description="""Une liste de typologies de service.
                Chaque résultat renvoyé a (au moins) une typologie dans cette liste."""
        ),
    ] = None,
    inclure_suspendus: Annotated[
        bool | SkipJsonSchema[None],
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
        code_insee = code_officiel_geographique.CODE_COMMUNE_BY_CODE_ARRONDISSEMENT.get(
            code_insee, code_insee
        )
        commune_instance = db_session.get(models.Commune, code_insee)
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

    if sources is None and source is not None:
        sources = [source]

    return search_services(
        request,
        db_session,
        sources=sources,
        commune_instance=commune_instance,
        thematiques=thematiques,
        frais=frais,
        types=types,
        search_point=search_point,
        include_outdated=inclure_suspendus,
    )


def create_token(email: str) -> schemas.Token:
    return schemas.Token(access=jwt.create_access_token(subject=email))


@v0_api_router.post(
    "/create_token",
    response_model=schemas.Token,
    include_in_schema=False,
)
def create_token_endpoint(
    token_creation_data: Annotated[schemas.TokenCreationData, fastapi.Body()],
    request: fastapi.Request,
):
    if "admin" in request.auth.scopes:
        return create_token(email=token_creation_data.email)
    else:
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized.",
        )


app = create_app()
