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
export AIRFLOW_CONN_S3="${AIRFLOW_CONN_S3}"
export AIRFLOW_CONN_PG="${AIRFLOW_CONN_PG}"

pip install .


airflow db upgrade
airflow users create \
    --email "${AIRFLOW_SUPERUSER_EMAIL}" \
    --role Admin \
    --firstname "${AIRFLOW_SUPERUSER_FIRSTNAME}" \
    --lastname "${AIRFLOW_SUPERUSER_LASTNAME}" \
    --password "${AIRFLOW_SUPERUSER_PASSWORD}" \
    --username "${AIRFLOW_SUPERUSER_USERNAME}"
airflow webserver --port "${PORT}" &
airflow scheduler

airflow pools set base_adresse_nationale_api 1 "Limit access to the ban api"