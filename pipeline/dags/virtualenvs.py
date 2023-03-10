from pathlib import Path

from airflow.configuration import get_airflow_home

VIRTUALENVS_DIR_PATH = Path(get_airflow_home()) / "venvs"

PYTHON_BIN_PATH = VIRTUALENVS_DIR_PATH / "python" / "venv/bin/python"
DBT_PYTHON_BIN_PATH = VIRTUALENVS_DIR_PATH / "dbt" / "venv/bin/python"
