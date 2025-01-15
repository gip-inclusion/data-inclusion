import logging

import pendulum

import airflow
from airflow.operators import empty, python

from dag_utils import date, sentry
from dag_utils.virtualenvs import PYTHON_BIN_PATH

logger = logging.getLogger(__name__)


def _import_dataset(
    run_id: str,
    logical_date,
):
    import pandas as pd

    from airflow.models import Variable
    from airflow.providers.amazon.aws.hooks import s3

    from dag_utils import pg

    ODSPEP_S3_KEY_PREFIX = Variable.get("ODSPEP_S3_KEY_PREFIX")

    s3_hook = s3.S3Hook(aws_conn_id="s3_sources")

    pg.create_schema("odspep")

    for excel_file_name in s3_hook.list_keys(prefix=ODSPEP_S3_KEY_PREFIX):
        tmp_filename = s3_hook.download_file(key=excel_file_name)

        df = pd.read_excel(tmp_filename, dtype=str, engine="openpyxl")
        df = df.assign(batch_id=run_id)
        df = df.assign(logical_date=logical_date)

        table_name = (
            excel_file_name.rstrip(".xlsx")
            .split("/")[-1]
            .replace(" ", "")
            .replace("-", "_")
            .upper()
        )

        with pg.connect_begin() as conn:
            df.to_sql(
                table_name,
                con=conn,
                schema="odspep",
                if_exists="replace",
                index=False,
            )


with airflow.DAG(
    dag_id="import_odspep",
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    default_args=sentry.notify_failure_args(),
    schedule="@once",
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
