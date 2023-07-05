from pathlib import Path

from airflow import configuration

VIRTUALENVS_DIR_PATH = Path(configuration.get_airflow_home()) / "venvs"

PYTHON_BIN_PATH = VIRTUALENVS_DIR_PATH / "python" / "venv/bin/python"
PIPX_PYTHON_BIN_PATH = VIRTUALENVS_DIR_PATH / "pipx" / "venv/bin/python"
DBT_PYTHON_BIN_PATH = VIRTUALENVS_DIR_PATH / "dbt" / "venv/bin/python"
