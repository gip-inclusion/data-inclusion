from pathlib import Path

from airflow.sdk import chain, dag, task, task_group
from airflow.task.trigger_rule import TriggerRule

from data_inclusion.pipeline.common import dags, dbt, s3
from data_inclusion.pipeline.dags.rename_services import tasks


@task.python
def export_dataset(to_s3_path: str):
    import json

    from airflow.providers.amazon.aws.hooks import s3
    from airflow.providers.postgres.hooks import postgres

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")
    s3_hook = s3.S3Hook(aws_conn_id="s3")

    for version in ["v1"]:
        for resource in ["structures", "services"]:
            key = (Path() / to_s3_path / version / resource).with_suffix(".parquet")
            query = f"SELECT * FROM public_marts.marts__{resource}_{version}"
            print(f"Downloading data from query='{query}'")
            df = pg_hook.get_df(sql=query)

            # this prevents conversion issues with nested JSON columns in Parquet
            if "_extra" in df.columns:
                df["_extra"] = df["_extra"].apply(
                    lambda x: json.dumps(x) if x is not None else None
                )

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
def run_main_pipeline():
    dbt_seed = dbt.dbt_task.override(task_id="dbt_seed")(command="seed")

    dbt_create_udfs = dbt.dbt_task.override(
        task_id="dbt_create_udfs",
    )(
        command="run-operation",
        macro="create_udfs",
    )

    SOURCES_STAGING_MODELS_PATH = (
        dbt.DBT_PROJECT_PATH / "models" / "staging" / "sources"
    )
    SOURCES_MAPPING_MODELS_PATH = (
        dbt.DBT_PROJECT_PATH / "models" / "intermediate" / "v1" / "001_mappings"
    )

    @task_group()
    def build_staging_and_mappings_models():
        for path in sorted(SOURCES_STAGING_MODELS_PATH.glob("*")):
            source_id = str(path).split("/")[-1].replace("_", "-")

            staging_model_path = path.relative_to(dbt.DBT_PROJECT_PATH)

            # not all sources have mapping models
            mapping_model_path = (
                (SOURCES_MAPPING_MODELS_PATH / path.name).relative_to(
                    dbt.DBT_PROJECT_PATH
                )
                if (SOURCES_MAPPING_MODELS_PATH / path.name).exists()
                else None
            )

            @task_group(group_id=source_id)
            def source_task_group():
                tasks = [
                    dbt.dbt_task.override(task_id="dbt_build_staging")(
                        command="build",
                        select=f"path:{staging_model_path}",
                    )
                ]

                if mapping_model_path is not None:
                    # two step build:
                    # 1. run the mapping in a temp table then run data tests
                    # 2. if data tests pass, run the mapping for real
                    tasks += [
                        dbt.dbt_task.override(
                            task_id="dbt_build_mapping_tmp",
                        )(
                            command="build",
                            select=f"path:{mapping_model_path}",
                            dbt_vars={"build_intermediate_tmp": True},
                        ),
                        dbt.dbt_task.override(
                            task_id="dbt_build_mapping",
                        )(
                            command="build",
                            select=f"path:{mapping_model_path}",
                        ),
                    ]

                chain(*tasks)

            source_task_group()

    dbt_build_unions = dbt.dbt_task.override(
        task_id="dbt_build_unions",
        trigger_rule=TriggerRule.ALL_DONE,
    )(
        command="build",
        select="intermediate.v1.002_unions",
    )

    @task_group()
    def build_enrichments_models():
        dbt_build_enrichments = dbt.dbt_task.override(
            task_id="dbt_build_enrichments",
        )(
            command="build",
            select="intermediate.v1.003_enrichments",
        )

        chain(
            [
                tasks.int__renommages_v1(incremental=True),
                dbt_build_enrichments,
            ]
        )

    dbt_build_finals = dbt.dbt_task.override(
        task_id="dbt_build_finals",
        trigger_rule=TriggerRule.ALL_DONE,
    )(
        command="build",
        select="intermediate.v1.004_finals",
    )

    dbt_build_deduplicate = dbt.dbt_task.override(
        task_id="dbt_build_deduplicate",
    )(
        command="build",
        select="intermediate.v1.005_deduplicate",
    )

    dbt_build_marts = dbt.dbt_task.override(task_id="dbt_build_marts")(
        command="build", select="marts.v1"
    )

    chain(
        dbt_seed,
        dbt_create_udfs,
        build_staging_and_mappings_models(),
        dbt_build_unions,
        build_enrichments_models(),
        dbt_build_finals,
        dbt_build_deduplicate,
        dbt_build_marts,
        export_dataset(to_s3_path=str(s3.get_key(stage="marts"))),
    )


run_main_pipeline()
