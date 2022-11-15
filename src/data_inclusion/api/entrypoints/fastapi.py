import logging
from typing import Optional

from sqlalchemy import orm

import fastapi
import fastapi_pagination
from fastapi.middleware import cors
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi_pagination.ext.sqlalchemy import paginate

from data_inclusion.api import models, schema, settings
from data_inclusion.api.core import db, jwt

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
    app = fastapi.FastAPI(
        title="data.inclusion API",
        description=description,
        docs_url="/api/v0/docs",
        contact={
            "name": "data.inclusion",
            "email": "data.inclusion@beta.gouv.fr",
            "url": "https://www.data.inclusion.beta.gouv.fr/",
        },
    )

    app.add_middleware(
        cors.CORSMiddleware,
        allow_origins=settings.CORS_ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(v0_api_router)
    app.include_router(v0_doc_api_router)

    fastapi_pagination.add_pagination(app)

    return app


def authenticated(token: HTTPAuthorizationCredentials = fastapi.Depends(HTTPBearer())):
    payload = jwt.verify_token(token.credentials)
    if payload is None:
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_403_FORBIDDEN,
            detail="Not authenticated",
        )


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
    query = db_session.query(models.Structure)

    if source is not None:
        query = query.filter_by(source=source)

    if id_ is not None:
        query = query.filter_by(id=id_)

    if departement is not None:
        query = query.filter(models.Structure.code_insee.startswith(departement.value))

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
        query = query.filter(models.Structure.thematiques.contains([thematique.value]))

    return list(paginate(query))


@v0_api_router.get(
    "/structures",
    response_model=fastapi_pagination.Page[schema.Structure],
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
    return [o.source for o in db_session.query(models.Structure.source).distinct()]


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
):
    query = db_session.query(
        models.Structure.source,
        models.Structure.id.label("structure_id"),
        models.Service.id,
        models.Service.nom,
        models.Service.presentation_resume,
        models.Service.types,
        models.Service.thematiques,
        models.Service.prise_rdv,
        models.Service.frais,
        models.Service.frais_autres,
        models.Service.profils,
    ).join(models.Service.structure)

    return list(paginate(query))


@v0_api_router.get(
    "/services",
    response_model=fastapi_pagination.Page[schema.Service],
    summary="Liste les services consolidées",
)
def list_services_endpoint(
    db_session=fastapi.Depends(db.get_session),
):
    """
    ## Liste les services consolidées par data.inclusion

    ### Retrouver la structure associée à un service donné

    Pour un service donné, il est possible de récupérer les informations de la structure
    associée en filtrant les structures par source et identifiant local:

    `/api/v0/structures/?source=<source>&id=<id>`
    """
    return list_services(db_session)


@v0_doc_api_router.get(
    "/labels_nationaux",
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
    "/typologies_services",
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
    "/typologies_structures",
    response_model=list[schema.EnhancedEnumMember],
    summary="Documente les typologies de structures",
)
def list_typologies_structures_endpoint():
    """
    ## Documente les typologies de structures
    """
    return schema.Typologie.as_dict_list()


app = create_app()
