#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${SCALINGO_POSTGRESQL_URL:-}" ]]; then
  export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${SCALINGO_POSTGRESQL_URL/postgres:\/\//postgresql:\/\/}"
fi

export AIRFLOW_HOME=.airflow/
export AIRFLOW__CORE__DAGS_FOLDER=dags
export AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
export AIRFLOW__CORE__DEFAULT_POOL_TASK_SLOT_COUNT=24
export AIRFLOW__CORE__DEFAULT_TIMEZONE=Europe/Paris
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export AIRFLOW__CORE__MAX_MAP_LENGTH=2048
export AIRFLOW__CORE__PARALLELISM=4
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=true
export AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL=60
export AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES=4
export AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY=0

exec airflow "$@"
