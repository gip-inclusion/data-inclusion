import logging

import airflow
import pendulum
from airflow.operators import empty, python
from airflow.providers.postgres.hooks.postgres import PostgresHook
from settings import SOURCES_CONFIGS

logger = logging.getLogger(__name__)


default_args = {}


def _setup(source_config: dict):
    pg_hook = PostgresHook(postgres_conn_id="pg")
    pg_engine = pg_hook.get_sqlalchemy_engine()
    schema_name = source_config["id"].replace("-", "_")

    with pg_engine.connect() as conn:
        with conn.begin():
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
            conn.execute(f"GRANT USAGE ON SCHEMA {schema_name} TO PUBLIC;")
            conn.execute(
                f"""\
                    ALTER DEFAULT PRIVILEGES IN SCHEMA {schema_name}
                    GRANT SELECT ON TABLES TO PUBLIC;
                """
            )


with airflow.DAG(
    dag_id="setup",
    start_date=pendulum.datetime(2022, 1, 1, tz="Europe/Paris"),
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    for source_config in SOURCES_CONFIGS:
        setup = python.PythonOperator(
            task_id=f"setup_{source_config['id']}".replace("-", "_"),
            python_callable=_setup,
            op_kwargs={"source_config": source_config},
        )

        start >> setup >> end
