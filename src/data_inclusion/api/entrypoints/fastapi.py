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


def create_app() -> fastapi.FastAPI:
    db.init_db()

    app = fastapi.FastAPI(
        title="data.inclusion API",
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
    tags=["Structures"],
)


def list_structures(
    db_session: orm.Session,
    typologie: Optional[schema.Typologie] = None,
    label_national: Optional[schema.LabelNational] = None,
) -> list:
    query = db_session.query(models.Structure)

    if typologie is not None:
        query = query.filter_by(typologie=typologie.value)

    if label_national is not None:
        query = query.filter(
            models.Structure.labels_nationaux.contains([label_national.value])
        )

    return list(paginate(query))


@v0_api_router.get(
    "/structures",
    response_model=fastapi_pagination.Page[schema.Structure],
)
def list_structures_endpoint(
    typologie: Optional[schema.Typologie] = None,
    label_national: Optional[schema.LabelNational] = None,
    db_session=fastapi.Depends(db.get_session),
):
    """
    ## Lister les structures consolidées par data.inclusion

    Il s'agit du point d'entrée principal de l'API, permettant d'accéder finement au
    données publiées quotidiennemnt en open data sur data.gouv.

    ### Token

    En production, un token d'accès est nécessaire et peut être obtenu en contactant
    l'équipe data.inclusion par mail ou sur leur mattermost betagouv.

    Le token doit être renseigné dans chaque requête via un header
    `Authorization: Bearer <VOTRE_TOKEN>`

    En staging, l'accès est libre.

    ### Schéma de données

    Les données respectent le schéma de data.inclusion. Plus d'informations sur le
    [dépôt](https://github.com/betagouv/data-inclusion-schema) versionnant le schéma,
    sur la [documentation officielle](https://www.data.inclusion.beta.gouv.fr/schemas-de-donnees-de-loffre/schema-des-structures-dinsertion)
    ou sur la page [schema.gouv](https://schema.data.gouv.fr/betagouv/data-inclusion-schema/) du schéma.


    ### Filtres

    Les structures peuvent être filtrées par typologie, label, source, etc.
    """  # noqa

    return list_structures(
        db_session,
        typologie=typologie,
        label_national=label_national,
    )


app = create_app()
