import logging

import airflow
import pandas as pd
import pendulum
from airflow.models import DagRun, Variable
from airflow.operators import empty, python
from airflow.providers.amazon.aws.hooks import s3
from airflow.providers.postgres.hooks import postgres
from airflow.models import Variable

logger = logging.getLogger(__name__)

default_args = {}

ODSPEP_S3_KEY_PREFIX = Variable.get("ODSPEP_S3_KEY_PREFIX")


def _import_dataset(
    run_id: str,
    dag_run: DagRun,
):
    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")
    pg_engine = pg_hook.get_sqlalchemy_engine()
    s3_hook = s3.S3Hook(aws_conn_id="s3_sources")

    logical_date = pendulum.instance(
        dag_run.logical_date.astimezone(dag.timezone)
    ).date()

    with pg_engine.connect() as conn:
        # put dataset in schema (for control access)
        with conn.begin():
            conn.execute("DROP SCHEMA IF EXISTS odspep CASCADE;")
            conn.execute("CREATE SCHEMA odspep;")

            # iterate over source files
            for s3_key in s3_hook.list_keys(prefix=ODSPEP_S3_KEY_PREFIX):
                # read in data
                tmp_filename = s3_hook.download_file(key=s3_key)
                df = pd.read_excel(tmp_filename, dtype=str, engine="openpyxl")

                # add metadata
                df = df.assign(batch_id=run_id)
                df = df.assign(logical_date=logical_date)

                # load to postgress
                table_name = (
                    s3_key.rstrip(".xlsx")
                    .split("/")[-1]
                    .replace(" ", "")
                    .replace("-", "_")
                    .upper()
                )
                df.to_sql(
                    table_name,
                    con=conn,
                    schema="odspep",
                    if_exists="replace",
                    index=False,
                )


with airflow.DAG(
    dag_id="import_odspep",
    start_date=pendulum.datetime(2022, 1, 1, tz="Europe/Paris"),
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    on_failure_callback=mm_failed_task,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    import_dataset = python.PythonOperator(
        task_id="import",
        python_callable=_import_dataset,
    )

    start >> import_dataset >> end
