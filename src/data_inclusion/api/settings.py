import os
from datetime import timedelta

import dotenv

dotenv.load_dotenv(dotenv.find_dotenv())

DATABASE_URL = os.environ["DATABASE_URL"]
SECRET_KEY = os.environ["SECRET_KEY"]
ACCESS_TOKEN_LIFETIME = timedelta(weeks=24)
CORS_ALLOWED_ORIGINS = ["*"]
TOKEN_ENABLED = os.environ.get("TOKEN_ENABLED") != "False"


DATAGOUV_API_URL = os.environ.get("DATAGOUV_API_URL")
DATAGOUV_DI_DATASET_ID = os.environ.get("DATAGOUV_DI_DATASET_ID")
DATAGOUV_API_KEY = os.environ.get("DATAGOUV_API_KEY")
DATAGOUV_DI_JSON_STRUCTURE_RESOURCE_ID = os.environ.get(
    "DATAGOUV_DI_JSON_STRUCTURE_RESOURCE_ID"
)
DATAGOUV_DI_CSV_STRUCTURE_RESOURCE_ID = os.environ.get(
    "DATAGOUV_DI_CSV_STRUCTURE_RESOURCE_ID"
)
DATAGOUV_DI_XLSX_STRUCTURE_RESOURCE_ID = os.environ.get(
    "DATAGOUV_DI_XLSX_STRUCTURE_RESOURCE_ID"
)