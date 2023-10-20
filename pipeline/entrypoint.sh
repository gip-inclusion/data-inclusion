#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Trace execution
[[ "${DEBUG}" ]] && set -x

BASE_DIR=$(pwd)

# The `DATABASE_URL` env var is automatically set by Scalingo and uses the depreciated
# scheme `postgres://`. Replace it.
export DATABASE_URL="${DATABASE_URL/postgres\:\/\//postgresql\:\/\/}"

export AIRFLOW_HOME="${BASE_DIR}/airflow"

export AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
export AIRFLOW__CORE__DAGS_FOLDER="${BASE_DIR}/dags"
export AIRFLOW__CORE__DEFAULT_TIMEZONE=Europe/Paris
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__FERNET_KEY="${SECRET_KEY}"
export AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${DATABASE_URL}"

# Create additional virtualenvs for isolated task executions
VIRTUAL_ENV="${AIRFLOW_HOME}/venvs/python/venv"
python -m venv "${VIRTUAL_ENV}"
"${VIRTUAL_ENV}/bin/python" -m pip install -U pip setuptools wheel
"${VIRTUAL_ENV}/bin/python" -m pip install -r requirements/tasks/python/requirements.txt
"${VIRTUAL_ENV}/bin/python" -m pip install .

# Create additional virtualenvs for isolated task executions
VIRTUAL_ENV="${AIRFLOW_HOME}/venvs/pipx/venv"
python -m venv "${VIRTUAL_ENV}"
"${VIRTUAL_ENV}/bin/python" -m pip install -U pip setuptools wheel
"${VIRTUAL_ENV}/bin/python" -m pip install -r requirements/tasks/pipx/requirements.txt

# Create additional virtualenvs for isolated task executions
VIRTUAL_ENV="${AIRFLOW_HOME}/venvs/dbt/venv"
python -m venv "${VIRTUAL_ENV}"
"${VIRTUAL_ENV}/bin/python" -m pip install -U pip setuptools wheel
"${VIRTUAL_ENV}/bin/python" -m pip install -r requirements/tasks/dbt/requirements.txt

# Install dbt packages (not python packages)
"${VIRTUAL_ENV}/bin/dbt" deps --project-dir "${AIRFLOW_VAR_DBT_PROJECT_DIR}"

airflow scheduler
