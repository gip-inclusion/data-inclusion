import pendulum

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from dag_utils import date
from dag_utils.dbt import dbt_operator_factory
from dag_utils.sentry import notify_failure_args
from dag_utils.sources import SOURCES_CONFIGS
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

    for resource in ["structures", "services"]:
        key = (base_prefix / resource).with_suffix(".parquet")
        query = f"SELECT * FROM public_marts.marts__{resource}"
        print(f"Downloading data from query='{query}'")
        df = pg_hook.get_pandas_df(sql=query)
        df.info()
        print(f"Uploading data to bucket='{key}'")
        s3_hook.load_bytes(
            bytes_data=df.to_parquet(compression="gzip"),
            key=str(key),
            replace=True,
        )


def get_staging_tasks():
    task_list = []

    for source_id in sorted(SOURCES_CONFIGS):
        dbt_source_id = source_id.replace("-", "_")

        stg_selector = f"path:models/staging/sources/**/*stg_{dbt_source_id}__*.sql"
        int_selector = (
            f"path:models/intermediate/001_mappings/**/*int_{dbt_source_id}__*.sql"
        )

        with TaskGroup(group_id=source_id) as source_task_group:
            dbt_run_staging = dbt_operator_factory(
                task_id="dbt_run_staging",
                command="run",
                select=stg_selector,
            )

            dbt_test_staging = dbt_operator_factory(
                task_id="dbt_test_staging",
                command="test",
                select=stg_selector,
            )

            dbt_build_intermediate_tmp = dbt_operator_factory(
                task_id="dbt_build_intermediate_tmp",
                command="build",
                select=int_selector,
                dbt_vars={"build_intermediate_tmp": True},
            )

            dbt_run_intermediate_mappings = dbt_operator_factory(
                task_id="dbt_run_intermediate_mappings",
                command="run",
                select=int_selector,
            )

            (
                dbt_run_staging
                >> dbt_test_staging
                >> dbt_build_intermediate_tmp
                >> dbt_run_intermediate_mappings
            )

        task_list += [source_task_group]

    return task_list


@dag(
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    default_args=notify_failure_args(),
    schedule="@hourly",
    catchup=False,
    concurrency=4,
)
def main():
    dbt_seed = dbt_operator_factory(
        task_id="dbt_seed",
        command="seed",
    )

    dbt_create_udfs = dbt_operator_factory(
        task_id="dbt_create_udfs",
        command="run-operation create_udfs",
    )

    dbt_build_intermediate_unions = dbt_operator_factory(
        task_id="dbt_build_intermediate_unions",
        command="build",
        select="intermediate.002_unions",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    dbt_build_intermediate_enrichments = dbt_operator_factory(
        task_id="dbt_build_intermediate_enrichments",
        command="build",
        select="intermediate.003_enrichments",
    )

    dbt_build_intermediate_finals = dbt_operator_factory(
        task_id="dbt_build_intermediate_finals",
        command="build",
        select="intermediate.004_finals",
    )

    dbt_build_intermediate_deduplicate = dbt_operator_factory(
        task_id="dbt_build_intermediate_deduplicate",
        command="build",
        select="intermediate.005_deduplicate",
    )

    dbt_build_marts = dbt_operator_factory(
        task_id="dbt_build_marts",
        command="build",
        select="marts",
    )

    dbt_snapshot_deduplicate_stats = dbt_operator_factory(
        task_id="dbt_snapshot_deduplicate_stats",
        command="snapshot",
        select="deduplicate",
    )

    chain(
        dbt_seed,
        dbt_create_udfs,
        get_staging_tasks(),
        dbt_build_intermediate_unions,
        dbt_build_intermediate_enrichments,
        dbt_build_intermediate_finals,
        dbt_build_intermediate_deduplicate,
        dbt_build_marts,
        dbt_snapshot_deduplicate_stats,
        export_dataset(),
    )


main()
