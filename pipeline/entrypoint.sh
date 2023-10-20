#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Trace execution
[[ "${DEBUG}" ]] && set -x

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
