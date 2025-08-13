import pendulum
from common import tasks

from airflow.decorators import dag, task

from dag_utils import date, sentry
from dag_utils.virtualenvs import PYTHON_BIN_PATH


@task.external_python(python=str(PYTHON_BIN_PATH))
def import_dataset(
    schema: str,
    run_id=None,
    logical_date=None,
):
    import pandas as pd

    from airflow.providers.amazon.aws.hooks import s3

    from dag_utils import pg

    ODSPEP_S3_KEY_PREFIX = "sources/odspep/2023-01-23/denormalized/Exports/"

    s3_hook = s3.S3Hook(aws_conn_id="s3")

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
                schema=schema,
                if_exists="replace",
                index=False,
            )


@dag(
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    default_args=sentry.notify_failure_args(),
    schedule="@once",
    catchup=False,
    tags=["source"],
)
def import_odspep():
    schema = "odspep"

    tasks.create_schema(name=schema) >> import_dataset(schema=schema)


import_odspep()
