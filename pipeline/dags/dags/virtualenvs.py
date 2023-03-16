from pathlib import Path

from airflow.models import Variable

VIRTUALENVS_DIR_PATH = Path(Variable.get("VENVS_DIR"))

PYTHON_BIN_PATH = VIRTUALENVS_DIR_PATH / "python" / "venv/bin/python"
DBT_PYTHON_BIN_PATH = VIRTUALENVS_DIR_PATH / "dbt" / "venv/bin/python"
