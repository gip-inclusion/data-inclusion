from pathlib import Path
from typing import Literal

from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.utils.trigger_rule import TriggerRule

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

    for version in ["v0", "v1"]:
        for resource in ["structures", "services"]:
            key = (Path() / to_s3_path / version / resource).with_suffix(".parquet")
            model = f"marts__{resource}_v1" if version == "v1" else f"marts__{resource}"
            query = f"SELECT * FROM public_marts.{model}"
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
    schedule="@hourly",
    max_active_tasks=4,
    **dags.common_args(use_sentry=True),
)
def main():
    dbt_seed = dbt.dbt_task.override(
        task_id="dbt_seed",
    )(
        command="seed",
    )

    dbt_create_udfs = dbt.dbt_task.override(
        task_id="dbt_create_udfs",
    )(
        command="run-operation",
        macro="create_udfs",
    )

    @task_group()
    def dbt_build_staging():
        for path in sorted(
            Path(dbt.DBT_PROJECT_PATH / "models" / "staging" / "sources").glob("*")
        ):
            path = path.relative_to(dbt.DBT_PROJECT_PATH)
            source_id = str(path).split("/")[-1].replace("_", "-")

            dbt.dbt_task.override(task_id=source_id)(
                command="build",
                select=f"path:{path}",
            )

    @task_group()
    def dbt_build_intermediate(version: Literal["v0", "v1"]):
        @task_group()
        def dbt_build_mappings():
            for path in sorted(
                Path(
                    dbt.DBT_PROJECT_PATH
                    / "models"
                    / "intermediate"
                    / version
                    / "001_mappings"
                ).glob("*")
            ):
                path = path.relative_to(dbt.DBT_PROJECT_PATH)
                source_id = str(path).split("/")[-1].replace("_", "-")

                @task_group(group_id=source_id)
                def source_mapping():
                    dbt_build_mapping_tmp = dbt.dbt_task.override(
                        task_id="dbt_build_mapping_tmp",
                        trigger_rule=TriggerRule.ALL_DONE,
                    )(
                        command="build",
                        select=f"path:{path}",
                        dbt_vars={"build_intermediate_tmp": True},
                    )
                    dbt_build_mapping = dbt.dbt_task.override(
                        task_id="dbt_build_mapping",
                    )(
                        command="build",
                        select=f"path:{path}",
                    )

                    chain(
                        dbt_build_mapping_tmp,
                        dbt_build_mapping,
                    )

                source_mapping()

        dbt_build_unions = dbt.dbt_task.override(
            task_id="dbt_build_unions",
            trigger_rule=TriggerRule.ALL_DONE,
        )(
            command="build",
            select=f"intermediate.{version}.002_unions",
        )

        dbt_build_enrichments = dbt.dbt_task.override(
            task_id="dbt_build_enrichments",
        )(
            command="build",
            select=f"intermediate.{version}.003_enrichments",
        )

        dbt_build_finals = dbt.dbt_task.override(
            task_id="dbt_build_finals",
        )(
            command="build",
            select=f"intermediate.{version}.004_finals",
        )

        dbt_build_deduplicate = dbt.dbt_task.override(
            task_id="dbt_build_deduplicate",
        )(
            command="build",
            select=f"intermediate.{version}.005_deduplicate",
        )

        chain(
            dbt_build_mappings(),
            dbt_build_unions,
            dbt_build_enrichments,
            dbt_build_finals,
            dbt_build_deduplicate,
        )

    dbt_snapshot_deduplicate_stats = dbt.dbt_task.override(
        task_id="dbt_snapshot_deduplicate_stats",
    )(
        command="snapshot",
        select="deduplicate",
    )

    chain(
        dbt_seed,
        dbt_create_udfs,
        dbt_build_staging(),
        dbt_build_intermediate.override(group_id="dbt_build_intermediate_v0")(
            version="v0"
        ),
        dbt_build_intermediate.override(group_id="dbt_build_intermediate_v1")(
            version="v1"
        ),
        [
            dbt.dbt_task.override(task_id="dbt_build_marts_v0")(
                command="build", select="marts.v0"
            ),
            dbt.dbt_task.override(task_id="dbt_build_marts_v1")(
                command="build", select="marts.v1"
            ),
        ],
        export_dataset(to_s3_path=str(s3.get_key(stage="marts"))),
        dbt_snapshot_deduplicate_stats,
    )


main()
