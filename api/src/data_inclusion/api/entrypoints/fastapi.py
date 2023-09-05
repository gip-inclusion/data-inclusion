import logging
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

from data_inclusion.api import models, schema, settings
from data_inclusion.api.core import auth, db, jwt
from data_inclusion.api.core.request.middleware import RequestMiddleware
from data_inclusion.api.utils import code_officiel_geographique, pagination

logger = logging.getLogger(__name__)

description = """### Token
* En production, un token d'accès est nécessaire et peut être obtenu en contactant
l'équipe data.inclusion par mail ou sur leur mattermost betagouv.

Le token doit être renseigné dans chaque requête via un header:
`Authorization: Bearer <VOTRE_TOKEN>`.

* En staging, l'accès est libre.

### Schémas de données

Les données respectent les schémas (structures et services) de data.inclusion.

Plus d'informations sur le
[dépôt](https://github.com/betagouv/data-inclusion-schema) versionnant le schéma,
sur la [documentation officielle](https://www.data.inclusion.beta.gouv.fr/schemas-de-donnees-de-loffre/schema-des-structures-dinsertion)
ou sur la page [schema.gouv](https://schema.data.gouv.fr/betagouv/data-inclusion-schema/) du schéma.
"""  # noqa: E501


def create_app() -> fastapi.FastAPI:
    # sentry must be initialized before app
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        traces_sample_rate=0,
        environment=settings.ENV,
    )

    app = fastapi.FastAPI(
        title="data.inclusion API",
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
            middleware.Middleware(RequestMiddleware),
        ],
    )

    app.include_router(v0_api_router)
    app.include_router(v0_doc_api_router)

    fastapi_pagination.add_pagination(app)

    return app


v0_api_router = fastapi.APIRouter(
    prefix="/api/v0",
    dependencies=[fastapi.Depends(HTTPBearer())] if settings.TOKEN_ENABLED else [],
    tags=["Données"],
)

v0_doc_api_router = fastapi.APIRouter(prefix="/api/v0/doc", tags=["Documentation"])


def list_structures(
    request: fastapi.Request,
    db_session: orm.Session,
    source: Optional[str] = None,
    id_: Optional[str] = None,
    typologie: Optional[schema.Typologie] = None,
    label_national: Optional[schema.LabelNational] = None,
    departement: Optional[schema.DepartementCOG] = None,
    departement_slug: Optional[schema.DepartementSlug] = None,
    code_postal: Optional[schema.CodePostal] = None,
    thematique: Optional[schema.Thematique] = None,
) -> list:
    query = sqla.select(models.Structure)

    if source is not None:
        query = query.filter_by(source=source)

    # FIXME: this is a temporary hack
    if request.user.username != "dora-staging-stream":
        query = query.filter(models.Structure.source != "agefiph")

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
                    schema.DepartementCOG[departement_slug.name].value
                ),
                models.Structure._di_geocodage_code_insee.startswith(
                    schema.DepartementCOG[departement_slug.name].value
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
    response_model=pagination.Page[schema.Structure],
    summary="Lister les structures consolidées",
)
def list_structures_endpoint(
    request: fastapi.Request,
    source: Annotated[str | SkipJsonSchema[None], fastapi.Query()] = None,
    id: Annotated[str | SkipJsonSchema[None], fastapi.Query()] = None,
    typologie: Annotated[
        schema.Typologie | SkipJsonSchema[None], fastapi.Query()
    ] = None,
    label_national: Annotated[
        schema.LabelNational | SkipJsonSchema[None], fastapi.Query()
    ] = None,
    thematique: Annotated[
        schema.Thematique | SkipJsonSchema[None], fastapi.Query()
    ] = None,
    departement: Annotated[
        schema.DepartementCOG | SkipJsonSchema[None], fastapi.Query()
    ] = None,
    departement_slug: Annotated[
        schema.DepartementSlug | SkipJsonSchema[None], fastapi.Query()
    ] = None,
    code_postal: Annotated[
        schema.CodePostal | SkipJsonSchema[None], fastapi.Query()
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
    response_model=schema.DetailedStructure,
    summary="Détailler une structure",
)
def retrieve_structure_endpoint(
    source: Annotated[str, fastapi.Path()],
    id: Annotated[str, fastapi.Path()],
    db_session=fastapi.Depends(db.get_session),
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


def list_sources(
    db_session: orm.Session,
) -> list[str]:
    query = sqla.select(models.Structure.source).distinct()
    query = query.order_by(models.Structure.source)
    return db_session.scalars(query).all()


@v0_api_router.get(
    "/sources",
    response_model=list[str],
    summary="Lister les sources consolidées",
)
def list_sources_endpoint(
    db_session=fastapi.Depends(db.get_session),
):
    return list_sources(db_session)


def list_services(
    request: fastapi.Request,
    db_session: orm.Session,
    source: Optional[str] = None,
    thematique: Optional[schema.Thematique] = None,
    departement: Optional[schema.DepartementCOG] = None,
    departement_slug: Optional[schema.DepartementSlug] = None,
    code_insee: Optional[schema.CodeInsee] = None,
):
    query = (
        sqla.select(models.Service)
        .join(models.Service.structure)
        .options(orm.contains_eager(models.Service.structure))
    )

    if source is not None:
        query = query.filter(models.Structure.source == source)

    # FIXME: this is a temporary hack
    if request.user.username != "dora-staging-stream":
        query = query.filter(models.Service.source != "agefiph")

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
                    schema.DepartementCOG[departement_slug.name].value
                ),
                models.Service._di_geocodage_code_insee.startswith(
                    schema.DepartementCOG[departement_slug.name].value
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
    response_model=pagination.Page[schema.Service],
    summary="Lister les services consolidées",
)
def list_services_endpoint(
    request: fastapi.Request,
    db_session=fastapi.Depends(db.get_session),
    source: Annotated[str | SkipJsonSchema[None], fastapi.Query()] = None,
    thematique: Annotated[
        schema.Thematique | SkipJsonSchema[None], fastapi.Query()
    ] = None,
    departement: Annotated[
        schema.DepartementCOG | SkipJsonSchema[None], fastapi.Query()
    ] = None,
    departement_slug: Annotated[
        schema.DepartementSlug | SkipJsonSchema[None], fastapi.Query()
    ] = None,
    code_insee: Annotated[
        schema.CodeInsee | SkipJsonSchema[None], fastapi.Query()
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
    response_model=schema.DetailedService,
    summary="Détailler un service",
)
def retrieve_service_endpoint(
    source: Annotated[str, fastapi.Path()],
    id: Annotated[str, fastapi.Path()],
    db_session=fastapi.Depends(db.get_session),
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
    thematiques: Optional[list[schema.Thematique]] = None,
    frais: Optional[list[schema.Frais]] = None,
    types: Optional[list[schema.TypologieService]] = None,
):
    query = (
        sqla.select(models.Service)
        .join(models.Service.structure)
        .options(orm.contains_eager(models.Service.structure))
    )

    if sources is not None:
        query = query.filter(models.Service.source == sqla.any_(sqla.literal(sources)))

    # FIXME: this is a temporary hack
    if request.user.username != "dora-staging-stream":
        query = query.filter(models.Service.source != "agefiph")

    if commune_instance is not None:
        # filter by zone de diffusion
        query = query.filter(
            sqla.or_(
                models.Service.zone_diffusion_type.is_(None),
                models.Service.zone_diffusion_type == schema.TypeCOG.PAYS.value,
                sqla.and_(
                    models.Service.zone_diffusion_type == schema.TypeCOG.COMMUNE.value,
                    models.Service.zone_diffusion_code == commune_instance.code,
                ),
                sqla.and_(
                    models.Service.zone_diffusion_type == schema.TypeCOG.EPCI.value,
                    sqla.literal(commune_instance.siren_epci).contains(
                        models.Service.zone_diffusion_code
                    ),
                ),
                sqla.and_(
                    models.Service.zone_diffusion_type
                    == schema.TypeCOG.DEPARTEMENT.value,
                    models.Service.zone_diffusion_code == commune_instance.departement,
                ),
                sqla.and_(
                    models.Service.zone_diffusion_type == schema.TypeCOG.REGION.value,
                    models.Service.zone_diffusion_code == commune_instance.region,
                ),
            )
        )

        query = query.filter(
            sqla.or_(
                # either `en-presentiel` within 100km
                geoalchemy2.functions.ST_DWithin(
                    sqla.cast(
                        geoalchemy2.functions.ST_MakePoint(
                            models.Service.longitude, models.Service.latitude
                        ),
                        geoalchemy2.Geography(geometry_type="GEOMETRY", srid=4326),
                    ),
                    sqla.select(
                        sqla.cast(
                            geoalchemy2.functions.ST_Simplify(
                                models.Commune.geom, 0.01
                            ),
                            geoalchemy2.Geography(geometry_type="GEOMETRY", srid=4326),
                        )
                    )
                    .filter(models.Commune.code == commune_instance.code)
                    .scalar_subquery(),
                    100_000,  # meters
                ),
                # or `a-distance`
                models.Service.modes_accueil.contains(
                    sqla.literal([schema.ModeAccueil.A_DISTANCE.value])
                ),
            )
        )

        # annotate distance
        query = query.add_columns(
            (
                sqla.case(
                    (
                        models.Service.modes_accueil.contains(
                            sqla.literal([schema.ModeAccueil.EN_PRESENTIEL.value])
                        ),
                        (
                            geoalchemy2.functions.ST_Distance(
                                sqla.cast(
                                    geoalchemy2.functions.ST_MakePoint(
                                        models.Service.longitude,
                                        models.Service.latitude,
                                    ),
                                    geoalchemy2.Geography(
                                        geometry_type="GEOMETRY", srid=4326
                                    ),
                                ),
                                sqla.select(
                                    sqla.cast(
                                        models.Commune.geom,
                                        geoalchemy2.Geography(
                                            geometry_type="GEOMETRY", srid=4326
                                        ),
                                    )
                                )
                                .filter(models.Commune.code == commune_instance.code)
                                .scalar_subquery(),
                            )
                            / 1000
                        ).cast(
                            sqla.Integer
                        ),  # conversion to kms
                    ),
                    else_=sqla.null().cast(sqla.Integer),
                )
            ).label("distance")
        )
    else:
        query = query.add_columns(sqla.null().cast(sqla.Integer).label("distance"))

    if thematiques is not None:
        filter_stmt = """\
        EXISTS(
            SELECT FROM unnest(service.thematiques) thematique
            WHERE
                EXISTS(
                    SELECT FROM unnest(:thematiques) input_thematique
                    WHERE thematique ^@ input_thematique
                )
        )
        """
        query = query.filter(
            sqla.text(filter_stmt).bindparams(
                thematiques=[t.value for t in thematiques]
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

    query = query.order_by(sqla.column("distance").nulls_last())

    def _items_to_mappings(items: list) -> list[dict]:
        # convert rows returned by `Session.execute` to a list of dicts that will be
        # used to instanciate pydantic models
        return [{"service": item[0], "distance": item[1]} for item in items]

    return paginate(db_session, query, unique=False, transformer=_items_to_mappings)


@v0_api_router.get(
    "/search/services",
    response_model=pagination.Page[schema.ServiceSearchResult],
    summary="Rechercher des services",
)
def search_services_endpoint(
    request: fastapi.Request,
    db_session=fastapi.Depends(db.get_session),
    source: Annotated[
        str | SkipJsonSchema[None],
        fastapi.Query(
            description="""Un identifiant de source. Déprécié en faveur de `sources`.""",
            deprecated=True,
        ),
    ] = None,
    sources: Annotated[
        list[str] | SkipJsonSchema[None],
        fastapi.Query(
            description="""Une liste d'identifiants de source.
                La liste des identifiants de source est disponible sur le endpoint dédié.
                Les résultats seront limités aux sources spécifiées.
            """,
        ),
    ] = None,
    code_insee: Annotated[
        schema.CodeInsee | SkipJsonSchema[None],
        fastapi.Query(
            description="""Code insee de la commune considérée.
                Si fourni, les résultats inclus également les services proches de cette commune.
                Les résultats sont triés par ordre de distance croissante.
            """
        ),
    ] = None,
    thematiques: Annotated[
        list[schema.Thematique] | SkipJsonSchema[None],
        fastapi.Query(
            description="""Une liste de thématique.
                Chaque résultat renvoyé a (au moins) une thématique dans cette liste."""
        ),
    ] = None,
    frais: Annotated[
        list[schema.Frais] | SkipJsonSchema[None],
        fastapi.Query(
            description="""Une liste de frais.
                Chaque résultat renvoyé a (au moins) un frais dans cette liste."""
        ),
    ] = None,
    types: Annotated[
        list[schema.TypologieService] | SkipJsonSchema[None],
        fastapi.Query(
            description="""Une liste de typologies de service.
                Chaque résultat renvoyé a (au moins) une typologie dans cette liste."""
        ),
    ] = None,
):
    """
    ## Rechercher des services

    La recherche de services permet de trouver des services dans une commune et à
    proximité.

    Les services peuvent être filtrés selon par thématiques, frais, typologies et
    code_insee de commune.

    En particulier, lorsque le `code_insee` d'une commune est fourni :

    * les services sont filtrés par zone de diffusion lorsque celle-ci est définie.
    * de plus, les services en présentiel sont filtrés dans un rayon de 100km autour de
    la commune.
    * le champ `distance` est :
        * rempli pour les services (non exclusivement) en présentiel.
        * laissé vide pour les services à distance et par défaut si le mode d'accueil
        n'est pas définie.
    * les résultats sont triés par distance croissante.
    """

    commune_instance = None
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
    )


@v0_doc_api_router.get(
    "/labels-nationaux",
    response_model=list[schema.EnhancedEnumMember],
    summary="Documente les labels nationaux",
)
def list_labels_nationaux_endpoint():
    """
    ## Documente les labels nationaux
    """
    return schema.LabelNational.as_dict_list()


@v0_doc_api_router.get(
    "/thematiques",
    response_model=list[schema.EnhancedEnumMember],
    summary="Documente les thématiques",
)
def list_thematiques_endpoint():
    """
    ## Documente les thématiques
    """
    return schema.Thematique.as_dict_list()


@v0_doc_api_router.get(
    "/typologies-services",
    response_model=list[schema.EnhancedEnumMember],
    summary="Documente les typologies de services",
)
def list_typologies_services_endpoint():
    """
    ## Documente les typologies de services
    """
    return schema.TypologieService.as_dict_list()


@v0_doc_api_router.get(
    "/frais",
    response_model=list[schema.EnhancedEnumMember],
    summary="Documente les frais",
)
def list_frais_endpoint():
    """
    ## Documente les frais
    """
    return schema.Frais.as_dict_list()


@v0_doc_api_router.get(
    "/profils",
    response_model=list[schema.EnhancedEnumMember],
    summary="Documente les profils de publics",
)
def list_profils_endpoint():
    """
    ## Documente les profils de publics
    """
    return schema.Profil.as_dict_list()


@v0_doc_api_router.get(
    "/typologies-structures",
    response_model=list[schema.EnhancedEnumMember],
    summary="Documente les typologies de structures",
)
def list_typologies_structures_endpoint():
    """
    ## Documente les typologies de structures
    """
    return schema.Typologie.as_dict_list()


@v0_doc_api_router.get(
    "/modes-accueil",
    response_model=list[schema.EnhancedEnumMember],
    summary="Documente les modes d'accueil",
)
def list_modes_accueil_endpoint():
    """
    ## Documente les modes d'accueil
    """
    return schema.ModeAccueil.as_dict_list()


@v0_doc_api_router.get(
    "/modes-orientation-accompagnateur",
    response_model=list[schema.EnhancedEnumMember],
    summary="Documente les modes d'orientation de l'accompagnateur",
)
def list_modes_orientation_accompagnateur_endpoint():
    """
    ## Documente les modes d'orientation de l'accompagnateur
    """
    return schema.ModeOrientationAccompagnateur.as_dict_list()


@v0_doc_api_router.get(
    "/modes-orientation-beneficiaire",
    response_model=list[schema.EnhancedEnumMember],
    summary="Documente les modes d'orientation du bénéficiaire",
)
def list_modes_orientation_beneficiaire_endpoint():
    """
    ## Documente les modes d'orientation du bénéficiaire
    """
    return schema.ModeOrientationBeneficiaire.as_dict_list()


def create_token(email: str) -> schema.Token:
    return schema.Token(access=jwt.create_access_token(subject=email))


@v0_api_router.post(
    "/create_token",
    response_model=schema.Token,
    include_in_schema=False,
)
def create_token_endpoint(
    token_creation_data: Annotated[schema.TokenCreationData, fastapi.Body()],
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
