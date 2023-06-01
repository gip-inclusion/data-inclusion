import logging

import airflow
import pendulum
from airflow.operators import empty, python

from dags.virtualenvs import PYTHON_BIN_PATH

logger = logging.getLogger(__name__)

default_args = {}


def _import_dataset(
    run_id: str,
    logical_date,
):
    import pandas as pd
    import pendulum
    from airflow.models import Variable
    from airflow.providers.amazon.aws.hooks import s3
    from airflow.providers.postgres.hooks import postgres

    IMMERSION_FACILITEE_S3_KEY_PREFIX = Variable.get(
        "IMMERSION_FACILITEE_S3_KEY_PREFIX"
    )

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")
    pg_engine = pg_hook.get_sqlalchemy_engine()
    s3_hook = s3.S3Hook(aws_conn_id="s3_sources")

    logical_date = pendulum.instance(
        logical_date.astimezone(pendulum.timezone("Europe/Paris"))
    ).date()

    with pg_engine.connect() as conn:
        # put dataset in schema (for control access)
        with conn.begin():
            conn.execute("DROP SCHEMA IF EXISTS immersion_facilitee CASCADE;")
            conn.execute("CREATE SCHEMA immersion_facilitee;")

            # iterate over source files
            for s3_key in s3_hook.list_keys(prefix=IMMERSION_FACILITEE_S3_KEY_PREFIX):
                # read in data
                tmp_filename = s3_hook.download_file(key=s3_key)
                df = pd.read_excel(tmp_filename, dtype=str, engine="openpyxl")

                # add metadata
                df = df.assign(batch_id=run_id)
                df = df.assign(logical_date=logical_date)

                # load to postgress
                table_name = s3_key.removesuffix(".xlsx").split("/")[-1]
                df.to_sql(
                    table_name,
                    con=conn,
                    schema="immersion_facilitee",
                    if_exists="replace",
                    index=False,
                )


with airflow.DAG(
    dag_id="import_immersion_facilitee",
    start_date=pendulum.datetime(2022, 1, 1, tz="Europe/Paris"),
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    tags=["source"],
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    import_dataset = python.ExternalPythonOperator(
        task_id="import",
        python=str(PYTHON_BIN_PATH),
        python_callable=_import_dataset,
    )

    start >> import_dataset >> end