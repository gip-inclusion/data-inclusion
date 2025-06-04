import pendulum

from airflow.decorators import dag, task
from airflow.operators import empty

from dag_utils import date
from dag_utils.dbt import (
    dbt_operator_factory,
    get_intermediate_tasks,
    get_staging_tasks,
)
from dag_utils.sentry import notify_failure_args
from dag_utils.virtualenvs import PYTHON_BIN_PATH


@task.external_python(
    python=str(PYTHON_BIN_PATH),
)
def export_dataset(
    logical_date,
    run_id,
):
    from pathlib import Path

    from airflow.providers.amazon.aws.hooks import s3
    from airflow.providers.postgres.hooks import postgres

    from dag_utils import date

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")
    s3_hook = s3.S3Hook(aws_conn_id="s3")

    base_prefix = Path("data") / "marts" / date.local_date_str(logical_date) / run_id

    for version in ["v0", "v1"]:
        for resource in ["structures", "services"]:
            if version == "v0":
                # for retro-compatibility, we keep the old key structure in v0
                key = (base_prefix / resource).with_suffix(".parquet")
                query = f"SELECT * FROM public_marts.marts__{resource}"
            else:
                key = (base_prefix / version / resource).with_suffix(".parquet")
                query = f"SELECT * FROM public_marts.marts__{resource}_{version}"
            print(f"Downloading data from query='{query}'")
            df = pg_hook.get_pandas_df(sql=query)
            df.info()
            print(f"Uploading data to bucket='{key}'")
            s3_hook.load_bytes(
                bytes_data=df.to_parquet(compression="gzip"),
                key=str(key),
                replace=True,
            )


@dag(
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    default_args=notify_failure_args(),
    schedule="@hourly",
    catchup=False,
    concurrency=4,
)
def main():
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    dbt_seed = dbt_operator_factory(
        task_id="dbt_seed",
        command="seed",
    )

    dbt_create_udfs = dbt_operator_factory(
        task_id="dbt_create_udfs",
        command="run-operation create_udfs",
    )

    snapshot_deduplicate_stats = dbt_operator_factory(
        task_id="snapshot_deduplicate_stats",
        command="snapshot",
        select="deduplicate",
    )

    (
        start
        >> dbt_seed
        >> dbt_create_udfs
        >> get_staging_tasks()
        >> get_intermediate_tasks()
        >> snapshot_deduplicate_stats
        >> export_dataset()
        >> end
    )


main()
