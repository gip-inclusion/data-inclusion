import logging
from typing import Annotated, Optional

import sentry_sdk
import sqlalchemy as sqla
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
from data_inclusion.api.utils import pagination

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

    return list(paginate(db_session, query))


@v0_api_router.get(
    "/structures",
    response_model=pagination.Page[schema.Structure],
    summary="Lister les structures consolidées",
)
def list_structures_endpoint(
    source: Optional[str] = None,
    id: Optional[str] = None,
    typologie: Optional[schema.Typologie] = None,
    label_national: Optional[schema.LabelNational] = None,
    thematique: Optional[schema.Thematique] = None,
    departement: Optional[schema.DepartementCOG] = None,
    departement_slug: Optional[schema.DepartementSlug] = None,
    code_postal: Optional[schema.CodePostal] = None,
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
    source: str,
    id: str,
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
    return [o.source for o in db_session.execute(query)]


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

    return list(paginate(db_session, query, unique=False))


@v0_api_router.get(
    "/services",
    response_model=pagination.Page[schema.Service],
    summary="Lister les services consolidées",
)
def list_services_endpoint(
    db_session=fastapi.Depends(db.get_session),
    source: Optional[str] = None,
    thematique: Optional[schema.Thematique] = None,
    departement: Optional[schema.DepartementCOG] = None,
    departement_slug: Optional[schema.DepartementSlug] = None,
    code_insee: Optional[schema.CodeInsee] = None,
):
    return list_services(
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
    source: str,
    id: str,
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
    db_session: orm.Session,
    source: Optional[str] = None,
    code_insee: Optional[schema.CodeInsee] = None,
    thematiques: Optional[list[schema.Thematique]] = None,
    frais: Optional[list[schema.Frais]] = None,
    types: Optional[list[schema.TypologieService]] = None,
):
    query = (
        sqla.select(models.Service)
        .join(models.Service.structure)
        .options(orm.contains_eager(models.Service.structure))
    )

    if source is not None:
        query = query.filter(models.Structure.source == source)

    if code_insee is not None:
        # for now, filter services that are not in the associated departement out
        cog_departement = code_insee[: 3 if code_insee.startswith("97") else 2]
        query = query.filter(
            sqla.or_(
                models.Service.code_insee.startswith(cog_departement),
                models.Service._di_geocodage_code_insee.startswith(cog_departement),
            )
        )

        coalesced_code_insee = sqla.func.coalesce(
            models.Service.code_insee, models.Service._di_geocodage_code_insee
        )

        # for now, assign an arbitrary distance based on the city code
        query = query.add_columns(
            sqla.case(
                (coalesced_code_insee == code_insee, 0),
                (coalesced_code_insee != code_insee, 40),
            ).label("distance")
        )
    else:
        query = query.add_columns(sqla.null().label("distance"))

    if thematiques is not None:
        filter_stmt = """\
        EXISTS(
            SELECT
            FROM unnest(service.thematiques) thematiques
            WHERE thematiques = ANY(:thematiques)
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

    query = query.order_by("distance")

    def _items_to_mappings(items: list) -> list[dict]:
        # convert rows returned by `Session.execute` to a list of dicts that will be
        # used to instanciate pydantic models
        return [{"service": item[0], "distance": item[1]} for item in items]

    return list(
        paginate(db_session, query, unique=False, transformer=_items_to_mappings)
    )


@v0_api_router.get(
    "/search/services",
    response_model=pagination.Page[schema.ServiceSearchResult],
    summary="Rechercher des services",
)
def search_services_endpoint(
    db_session=fastapi.Depends(db.get_session),
    source: Optional[str] = None,
    code_insee: Annotated[
        Optional[schema.CodeInsee],
        fastapi.Query(
            description="""Code insee de la commune considérée.
                Si fourni, les résultats inclus également les services proches de cette commune.
                Les résultats sont triés par ordre de distance croissante.
            """
        ),
    ] = None,
    thematiques: Annotated[
        Optional[list[schema.Thematique]],
        fastapi.Query(
            description="""Une liste de thématique.
                Chaque résultat renvoyé a (au moins) une thématique dans cette liste."""
        ),
    ] = None,
    frais: Annotated[
        Optional[list[schema.Frais]],
        fastapi.Query(
            description="""Une liste de frais.
                Chaque résultat renvoyé a (au moins) un frais dans cette liste."""
        ),
    ] = None,
    types: Annotated[
        Optional[list[schema.TypologieService]],
        fastapi.Query(
            description="""Une liste de typologies de service.
                Chaque résultat renvoyé a (au moins) une typologie dans cette liste."""
        ),
    ] = None,
):
    """
    ## Rechercher des services

    La recherche de services permet de trouver des services dans une commune et à proximité.

    Les services peuvent être filtrés selon par thématiques, frais, typologies.

    Lorsqu'un `code_insee` est fourni:

    * les services qui ne sont pas rattachés (par leur adresse ou celle de leur structure)
        au département du code insee de la recherche sont exclus,
    * null si un service n'a pas de code_insee,
    * 0 si code_insee du service correspond au code_insee de la recherche,
    * 40 sinon les code_insee sont différents,
    * les résultats sont triés par distance croissante.
    """  # noqa: W505
    return search_services(
        db_session,
        source=source,
        code_insee=code_insee,
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
    summary="Documente les modes d'orientation de l'beneficiaire",
)
def list_modes_orientation_beneficiaire_endpoint():
    """
    ## Documente les modes d'orientation de l'beneficiaire
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
    token_creation_data: schema.TokenCreationData,
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
