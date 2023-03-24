#!/bin/bash

# This script runs airflow locally and is used as an entrypoint for scalingo

# Exit immediately if a command exits with a non-zero status.
set -e

# Trace execution
[[ "${DEBUG}" ]] && set -x

if [[ -f .env ]]; then
    set -a
    source .env
    set +a
fi

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

airflow db upgrade
airflow users create \
    --email "${AIRFLOW_SUPERUSER_EMAIL}" \
    --role Admin \
    --firstname "${AIRFLOW_SUPERUSER_FIRSTNAME}" \
    --lastname "${AIRFLOW_SUPERUSER_LASTNAME}" \
    --password "${AIRFLOW_SUPERUSER_PASSWORD}" \
    --username "${AIRFLOW_SUPERUSER_USERNAME}"

# Run airflow services in the background, to be able to keep doing things.
# Once the webserver is up, the deployment on scalingo will be marked as successful and
# there will no timeout error raised (1min).
airflow webserver --port "${PORT}" &
airflow scheduler &

# Keep doing things

# Create additional virtualenvs for isolated task executions
VIRTUAL_ENV="${AIRFLOW_HOME}/venvs/python/venv"
python -m venv "${VIRTUAL_ENV}"
"${VIRTUAL_ENV}/bin/python" -m pip install -U pip setuptools wheel
"${VIRTUAL_ENV}/bin/python" -m pip install -r requirements/tasks/python/requirements.txt
"${VIRTUAL_ENV}/bin/python" -m pip install .

# Create additional virtualenvs for isolated task executions
VIRTUAL_ENV="${AIRFLOW_HOME}/venvs/dbt/venv"
python -m venv "${VIRTUAL_ENV}"
"${VIRTUAL_ENV}/bin/python" -m pip install -U pip setuptools wheel
"${VIRTUAL_ENV}/bin/python" -m pip install -r requirements/tasks/dbt/requirements.txt

# Install dbt packages (not python packages)
"${VIRTUAL_ENV}/bin/dbt" deps --project-dir "${AIRFLOW_VAR_DBT_PROJECT_DIR}"

airflow pools set base_adresse_nationale_api 1 "Limit access to the ban api"

# Keep the application container up
sleep infinity