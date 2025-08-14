from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

from data_inclusion.pipeline.common import dags, tasks


@task.external_python(python=tasks.PYTHON_BIN_PATH)
def import_dataset(
    schema: str,
    run_id=None,
    logical_date=None,
):
    import pandas as pd

    from airflow.providers.amazon.aws.hooks import s3
    from airflow.providers.postgres.hooks import postgres

    ODSPEP_S3_KEY_PREFIX = "sources/odspep/2023-01-23/denormalized/Exports/"

    s3_hook = s3.S3Hook(aws_conn_id="s3")
    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

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

        with pg_hook.get_sqlalchemy_engine().begin() as conn:
            df.to_sql(
                table_name,
                con=conn,
                schema=schema,
                if_exists="replace",
                index=False,
            )


@dag(
    schedule="@once",
    tags=["source"],
    **dags.common_args(use_sentry=True),
)
def import_odspep():
    schema = "odspep"

    chain(tasks.create_schema(name=schema), import_dataset(schema=schema))


import_odspep()
