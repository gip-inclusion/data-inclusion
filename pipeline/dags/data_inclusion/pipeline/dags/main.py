from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from data_inclusion.pipeline import sources
from data_inclusion.pipeline.common import dags, dbt, s3, tasks


@task.external_python(
    python=tasks.PYTHON_BIN_PATH,
)
def export_dataset(to_s3_path: str):
    from pathlib import Path

    from airflow.providers.amazon.aws.hooks import s3
    from airflow.providers.postgres.hooks import postgres

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")
    s3_hook = s3.S3Hook(aws_conn_id="s3")

    for resource in ["structures", "services"]:
        key = (Path() / to_s3_path / resource).with_suffix(".parquet")
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

    for source_id in sorted(sources.SOURCES_CONFIGS):
        dbt_source_id = source_id.replace("-", "_")

        stg_selector = f"path:models/staging/sources/**/*stg_{dbt_source_id}__*.sql"
        int_selector = (
            f"path:models/intermediate/001_mappings/**/*int_{dbt_source_id}__*.sql"
        )

        with TaskGroup(group_id=source_id) as source_task_group:
            dbt_run_staging = dbt.dbt_operator_factory(
                task_id="dbt_run_staging",
                command="run",
                select=stg_selector,
            )

            dbt_test_staging = dbt.dbt_operator_factory(
                task_id="dbt_test_staging",
                command="test",
                select=stg_selector,
            )

            dbt_build_intermediate_tmp = dbt.dbt_operator_factory(
                task_id="dbt_build_intermediate_tmp",
                command="build",
                select=int_selector,
                dbt_vars={"build_intermediate_tmp": True},
            )

            dbt_run_intermediate_mappings = dbt.dbt_operator_factory(
                task_id="dbt_run_intermediate_mappings",
                command="run",
                select=int_selector,
            )

            chain(
                dbt_run_staging,
                dbt_test_staging,
                dbt_build_intermediate_tmp,
                dbt_run_intermediate_mappings,
            )

        task_list += [source_task_group]

    return task_list


@dag(
    schedule="@hourly",
    max_active_tasks=4,
    **dags.common_args(use_sentry=True),
)
def main():
    dbt_seed = dbt.dbt_operator_factory(
        task_id="dbt_seed",
        command="seed",
    )

    dbt_create_udfs = dbt.dbt_operator_factory(
        task_id="dbt_create_udfs",
        command="run-operation create_udfs",
    )

    dbt_build_intermediate_unions = dbt.dbt_operator_factory(
        task_id="dbt_build_intermediate_unions",
        command="build",
        select="intermediate.002_unions",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    dbt_build_intermediate_enrichments = dbt.dbt_operator_factory(
        task_id="dbt_build_intermediate_enrichments",
        command="build",
        select="intermediate.003_enrichments",
    )

    dbt_build_intermediate_finals = dbt.dbt_operator_factory(
        task_id="dbt_build_intermediate_finals",
        command="build",
        select="intermediate.004_finals",
    )

    dbt_build_intermediate_deduplicate = dbt.dbt_operator_factory(
        task_id="dbt_build_intermediate_deduplicate",
        command="build",
        select="intermediate.005_deduplicate",
    )

    dbt_build_marts = dbt.dbt_operator_factory(
        task_id="dbt_build_marts",
        command="build",
        select="marts",
    )

    dbt_snapshot_deduplicate_stats = dbt.dbt_operator_factory(
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
        export_dataset(to_s3_path=str(s3.get_key(stage="marts"))),
    )


main()
