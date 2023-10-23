#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Trace execution
[[ "${DEBUG}" ]] && set -x

if [[ $# -eq 0 ]]; then
    echo "No service parameter provided.";
    exit 1;
fi

COMMAND=$1

# The `DATABASE_URL` env var is automatically set by Scalingo and uses the depreciated
# scheme `postgres://`. Therefore it is replaced.
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${DATABASE_URL/postgres\:\/\//postgresql\:\/\/}"

export AIRFLOW_HOME="${HOME}/airflow"
export AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
export AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__DEFAULT_TIMEZONE=Europe/Paris
export AIRFLOW__CORE__FERNET_KEY="${SECRET_KEY}"
export AIRFLOW__CORE__DAGS_FOLDER="${HOME}/dags"

export AIRFLOW_CONN_S3_LOGS="${AIRFLOW_CONN_S3_LOGS}"
export AIRFLOW__LOGGING__REMOTE_LOGGING=True
export AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://data-inclusion-lake/logs
export AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=s3_logs
export AIRFLOW__LOGGING__DELETE_LOCAL_LOGS=True

if [[ "${COMMAND}" = "webserver" ]]; then
    airflow webserver --port "${PORT}"
fi

if [[ "${COMMAND}" = "migrate" ]]; then
    airflow db migrate
fi

if [[ "${COMMAND}" = "scheduler" ]]; then
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
fi
