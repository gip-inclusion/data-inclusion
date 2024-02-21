import os

import dotenv

dotenv.load_dotenv(dotenv.find_dotenv())

DATABASE_URL = os.environ["DATABASE_URL"]
SECRET_KEY = os.environ["SECRET_KEY"]
CORS_ALLOWED_ORIGINS = ["*"]
TOKEN_ENABLED = os.environ.get("TOKEN_ENABLED") != "False"
SENTRY_DSN = os.environ.get("SENTRY_DSN")
ENV = os.environ.get("ENV", "prod")
SOLIGUIDE_API_URL = "https://api.soliguide.fr"
SOLIGUIDE_API_TOKEN = os.environ.get("SOLIGUIDE_API_TOKEN", None)

DEFAULT_PAGE_SIZE = 500
MAX_PAGE_SIZE = 1000
