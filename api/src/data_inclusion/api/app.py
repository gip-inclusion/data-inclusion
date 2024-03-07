import logging
from pathlib import Path

import sentry_sdk

import fastapi
import fastapi_pagination
from fastapi import middleware
from fastapi.middleware import cors
from fastapi.security import HTTPBearer

from data_inclusion.api import settings
from data_inclusion.api.auth.router import router as auth_api_router
from data_inclusion.api.core import auth, db
from data_inclusion.api.core.request.middleware import save_request_middleware
from data_inclusion.api.data.router import router as data_api_router
from data_inclusion.api.schema.router import router as schema_api_router

logger = logging.getLogger(__name__)


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

    fastapi_pagination.add_pagination(app)

    return app


v0_api_router = fastapi.APIRouter(
    prefix="/api/v0",
    dependencies=[fastapi.Depends(HTTPBearer())] if settings.TOKEN_ENABLED else [],
)

v0_api_router.include_router(
    data_api_router,
    tags=["Données"],
)
v0_api_router.include_router(
    auth_api_router,
    include_in_schema=False,
    tags=["Authentication"],
)
v0_api_router.include_router(
    schema_api_router,
    prefix="/doc",
    tags=["Documentation"],
)


app = create_app()
