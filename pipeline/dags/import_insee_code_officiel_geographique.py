import logging

import airflow
import pendulum
from airflow.operators import empty, python

from dag_utils.virtualenvs import PYTHON_BIN_PATH

logger = logging.getLogger(__name__)

default_args = {}


def _setup():
    from airflow.providers.postgres.hooks import postgres

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")
    pg_engine = pg_hook.get_sqlalchemy_engine()
    schema_name = "insee"

    with pg_engine.connect() as conn:
        with conn.begin():
            conn.execute(
                f"""\
                CREATE SCHEMA IF NOT EXISTS {schema_name};
                GRANT USAGE ON SCHEMA {schema_name} TO PUBLIC;
                ALTER DEFAULT PRIVILEGES IN SCHEMA {schema_name}
                GRANT SELECT ON TABLES TO PUBLIC;"""
            )


def _import_dataset_ressource(ressource_name: str):
    import pandas as pd
    from airflow.models import Variable
    from airflow.providers.postgres.hooks import postgres

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    url = (
        Variable.get("INSEE_COG_DATASET_URL").strip("/")
        + f"/v_{ressource_name[:-1]}_2023.csv"
    )

    df = pd.read_csv(url, sep=",", dtype=str)

    engine = pg_hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        df.to_sql(
            ressource_name,
            schema="insee",
            con=conn,
            if_exists="replace",
            index=False,
        )


with airflow.DAG(
    dag_id="import_insee_code_officiel_geographique",
    start_date=pendulum.datetime(2022, 1, 1, tz="Europe/Paris"),
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    setup = python.ExternalPythonOperator(
        task_id="setup",
        python=str(PYTHON_BIN_PATH),
        python_callable=_setup,
    )

    for ressource_name in ["regions", "departements", "communes"]:
        import_dataset_ressource = python.ExternalPythonOperator(
            task_id=f"import_{ressource_name}",
            python=str(PYTHON_BIN_PATH),
            python_callable=_import_dataset_ressource,
            op_kwargs={"ressource_name": ressource_name},
        )

        start >> setup >> import_dataset_ressource >> end
