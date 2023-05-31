import logging
from typing import Annotated, Optional

import sentry_sdk
import sqlalchemy as sqla
from sqlalchemy import orm

import fastapi
import fastapi_pagination
from fastapi import middleware, requests
from fastapi.middleware import cors
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi_pagination.ext.sqlalchemy import paginate

from data_inclusion.api import models, schema, settings
from data_inclusion.api.core import db, jwt
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
            middleware.Middleware(RequestMiddleware),
        ],
    )

    app.include_router(v0_api_router)
    app.include_router(v0_doc_api_router)

    fastapi_pagination.add_pagination(app)

    return app


def authenticated(
    request: requests.Request,
    token: HTTPAuthorizationCredentials = fastapi.Depends(HTTPBearer()),
) -> Optional[dict]:
    payload = jwt.verify_token(token.credentials)
    if payload is None:
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_403_FORBIDDEN,
            detail="Not authenticated",
        )

    # attach username to the request
    # TODO: https://www.starlette.io/authentication/ instead
    if "sub" in payload:
        request.scope["user"] = payload["sub"]
        request.scope["admin"] = payload.get("admin", False)

    return payload


v0_api_router = fastapi.APIRouter(
    prefix="/api/v0",
    dependencies=[fastapi.Depends(authenticated)] if settings.TOKEN_ENABLED else [],
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
            models.Structure.code_insee.startswith(
                schema.DepartementCOG[departement_slug.name].value
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
    """
    ## Lister les sources consolidées
    """
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
        sqla.select(
            models.Structure.source,
            models.Structure.id.label("structure_id"),
            models.Service._di_surrogate_id,
            models.Service.id,
            models.Service.nom,
            models.Service.presentation_resume,
            models.Service.presentation_detail,
            models.Service.types,
            models.Service.thematiques,
            models.Service.prise_rdv,
            models.Service.frais,
            models.Service.frais_autres,
            models.Service.profils,
            models.Service.pre_requis,
            models.Service.cumulable,
            models.Service.justificatifs,
            models.Service.formulaire_en_ligne,
            models.Service.commune,
            models.Service.code_postal,
            models.Service.code_insee,
            models.Service.adresse,
            models.Service.complement_adresse,
            models.Service.longitude,
            models.Service.latitude,
            models.Service.recurrence,
            models.Service.date_creation,
            models.Service.date_suspension,
            models.Service.lien_source,
            models.Service.telephone,
            models.Service.courriel,
            models.Service.contact_public,
            models.Service.date_maj,
            models.Service.modes_accueil,
            models.Service.zone_diffusion_type,
            models.Service.zone_diffusion_code,
            models.Service.zone_diffusion_nom,
        )
        .select_from(models.Service)
        .join(models.Structure)
    )

    if source is not None:
        query = query.filter(models.Structure.source == source)

    if departement is not None:
        query = query.filter(models.Service.code_insee.startswith(departement.value))

    if departement_slug is not None:
        query = query.filter(
            models.Service.code_insee.startswith(
                schema.DepartementCOG[departement_slug.name].value
            )
        )

    if code_insee is not None:
        query = query.filter(models.Service.code_insee == code_insee)

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
    summary="Liste les services consolidées",
)
def list_services_endpoint(
    db_session=fastapi.Depends(db.get_session),
    source: Optional[str] = None,
    thematique: Optional[schema.Thematique] = None,
    departement: Optional[schema.DepartementCOG] = None,
    departement_slug: Optional[schema.DepartementSlug] = None,
    code_insee: Optional[schema.CodeInsee] = None,
):
    """
    ## Liste les services consolidées par data.inclusion

    ### Retrouver la structure associée à un service donné

    Pour un service donné, il est possible de récupérer les informations de la structure
    associée en filtrant les structures par source et identifiant local:

    `/api/v0/structures/?source=<source>&id=<id>`
    """
    return list_services(
        db_session,
        source=source,
        thematique=thematique,
        departement=departement,
        departement_slug=departement_slug,
        code_insee=code_insee,
    )


@v0_api_router.get(
    "/services/{_di_surrogate_id}",
    response_model=schema.DetailedService,
    summary="Détaille un service",
)
def retrieve_service_endpoint(
    _di_surrogate_id: Annotated[
        str, fastapi.Path(description="L'identifiant créé par data.inclusion.")
    ],
    db_session=fastapi.Depends(db.get_session),
):
    service_instance = db_session.scalars(
        sqla.select(models.Service)
        .options(orm.selectinload(models.Service.structure))
        .filter_by(_di_surrogate_id=_di_surrogate_id)
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
        sqla.select(
            models.Structure.source,
            models.Structure.id.label("structure_id"),
            models.Service._di_surrogate_id,
            models.Service.id,
            models.Service.nom,
            models.Service.presentation_resume,
            models.Service.presentation_detail,
            models.Service.types,
            models.Service.thematiques,
            models.Service.prise_rdv,
            models.Service.frais,
            models.Service.frais_autres,
            models.Service.profils,
            models.Service.pre_requis,
            models.Service.cumulable,
            models.Service.justificatifs,
            models.Service.formulaire_en_ligne,
            models.Service.commune,
            models.Service.code_postal,
            models.Service.code_insee,
            models.Service.adresse,
            models.Service.complement_adresse,
            models.Service.longitude,
            models.Service.latitude,
            models.Service.recurrence,
            models.Service.date_creation,
            models.Service.date_suspension,
            models.Service.lien_source,
            models.Service.telephone,
            models.Service.courriel,
            models.Service.contact_public,
            models.Service.date_maj,
            models.Service.modes_accueil,
            models.Service.zone_diffusion_type,
            models.Service.zone_diffusion_code,
            models.Service.zone_diffusion_nom,
        )
        .select_from(models.Service)
        .join(models.Structure)
    )

    if source is not None:
        query = query.filter(models.Structure.source == source)

    if code_insee is not None:
        # for now, filter services that are not in the associated departement out
        cog_departement = code_insee[: 3 if code_insee.startswith("97") else 2]
        query = query.filter(
            models.Service.code_insee.startswith(cog_departement)
            | models.Structure.code_insee.startswith(cog_departement)
        )

        # for now, assign an arbitrary distance based on the city code
        query = query.add_columns(
            sqla.case(
                (models.Service.code_insee.is_(sqla.null()), sqla.null()),
                (models.Service.code_insee == code_insee, 0),
                (models.Service.code_insee != code_insee, 40),
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

    return list(paginate(db_session, query, unique=False))


@v0_api_router.get(
    "/search/services",
    response_model=pagination.Page[schema.ServiceSearchResult],
    summary="Recherche de services",
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
    ## Recherche de services

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
    if request.scope.get("admin"):
        return create_token(email=token_creation_data.email)
    else:
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized.",
        )


app = create_app()
