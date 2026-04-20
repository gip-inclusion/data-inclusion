from pathlib import Path

import jinja2
import sentry_sdk

import fastapi
from fastapi.middleware import cors, gzip, trustedhost

from data_inclusion.api import auth, config
from data_inclusion.api.auth.routes import router as auth_api_router
from data_inclusion.api.core import db
from data_inclusion.api.inclusion_data.routes import router as data_api_router
from data_inclusion.api.inclusion_schema.routes import router as schema_api_router
from data_inclusion.api.valideur.routes import app as valideur_app
from data_inclusion.api.widget.routes import app as widget_app

API_DESCRIPTION_PATH = Path(__file__).parent / "api_description.md"


def setup_debug_toolbar_middleware(app: fastapi.FastAPI) -> None:
    try:
        from debug_toolbar.middleware import DebugToolbarMiddleware
    except ImportError:
        # some tests force the "dev" env, with debug toolbar not installed
        return

    app.add_middleware(
        DebugToolbarMiddleware,
        panels=["debug_toolbar.panels.sqlalchemy.SQLAlchemyPanel"],
    )


def create_app(settings: config.Settings) -> fastapi.FastAPI:
    # sentry must be initialized before app
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        send_default_pii=True,  # get IP address & headers, useful to identify bots
        traces_sample_rate=0.1,
        profiles_sample_rate=0.1,  # 10% of sampled requests will also be profiled
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
        docs_url="/api/docs",
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

    app.add_middleware(gzip.GZipMiddleware)

    app.add_middleware(
        cors.CORSMiddleware,
        allow_origins=settings.CORS_ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    if settings.ENV == "dev":
        setup_debug_toolbar_middleware(app)

    if settings.ENV in ["prod", "staging"]:
        app.add_middleware(
            trustedhost.TrustedHostMiddleware,
            allowed_hosts=settings.ALLOWED_HOSTS,
        )

    app.middleware("http")(db.db_session_middleware)

    app.include_router(api_router)
    app.include_router(auth_api_router, prefix="/auth", include_in_schema=False)
    app.mount(path="/widget", app=widget_app)
    app.mount(path="/valideur", app=valideur_app)

    @app.get("/robots.txt", include_in_schema=False)
    def get_robots_txt():
        content = "User-agent: *\nAllow: /api/docs\nDisallow: /\n"
        return fastapi.Response(content=content, media_type="text/plain")

    return app


api_router = fastapi.APIRouter(prefix="/api/v1")
api_router.include_router(data_api_router)
api_router.include_router(schema_api_router, prefix="/doc")

app = create_app(settings=config.settings)
