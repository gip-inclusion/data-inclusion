from pathlib import Path

import jinja2
import sentry_sdk

import fastapi
import fastapi_pagination
from fastapi.middleware import cors

from data_inclusion.api import auth
from data_inclusion.api.auth.routes import router as auth_api_router
from data_inclusion.api.config import settings
from data_inclusion.api.core import db
from data_inclusion.api.inclusion_data.routes import router as data_api_router
from data_inclusion.api.inclusion_schema.routes import router as schema_api_router
from data_inclusion.api.request.middleware import save_request_middleware

API_DESCRIPTION_PATH = Path(__file__).parent / "api_description.md"


def setup_cors_middleware(app: fastapi.FastAPI) -> None:
    app.add_middleware(
        cors.CORSMiddleware,
        allow_origins=settings.CORS_ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


def setup_debug_toolbar_middleware(app: fastapi.FastAPI) -> None:
    from debug_toolbar.middleware import DebugToolbarMiddleware

    app.add_middleware(
        DebugToolbarMiddleware,
        panels=["debug_toolbar.panels.sqlalchemy.SQLAlchemyPanel"],
    )


def create_app() -> fastapi.FastAPI:
    # sentry must be initialized before app
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        traces_sample_rate=1.0,
        enable_tracing=True,
        environment=settings.ENV,
    )

    description = jinja2.Template(
        API_DESCRIPTION_PATH.read_text(), autoescape=False
    ).render(base_url=settings.BASE_URL)
    app = fastapi.FastAPI(
        title="data·inclusion API",
        servers=[{"url": settings.BASE_URL, "description": settings.ENV}],
        openapi_url="/api/openapi.json",
        description=description,
        docs_url="/api/v0/docs",
        contact={
            "name": "data·inclusion",
            "email": "data-inclusion@inclusion.gouv.fr",
            "url": "https://data.inclusion.gouv.fr/",
        },
        debug=settings.DEBUG,
        swagger_ui_parameters={
            # this makes the search services endpoint first
            "operationsSorter": "alpha",
        },
        dependencies=[auth.authenticate_dependency],
    )

    setup_cors_middleware(app)

    if settings.ENV == "dev":
        setup_debug_toolbar_middleware(app)

    app.middleware("http")(db.db_session_middleware)
    app.middleware("http")(save_request_middleware)

    app.include_router(v0_api_router)
    app.include_router(
        schema_api_router,
        prefix="/api/v0/doc",
        tags=["Documentation"],
    )

    fastapi_pagination.add_pagination(app)

    return app


v0_api_router = fastapi.APIRouter(prefix="/api/v0")

v0_api_router.include_router(data_api_router)
v0_api_router.include_router(auth_api_router, include_in_schema=False)


app = create_app()
