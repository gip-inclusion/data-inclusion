from airflow.sdk import chain, dag, task

from data_inclusion.pipeline.common import dags, dbt, tasks
from data_inclusion.pipeline.dags.import_sirene import constants


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
)
def import_file(url: str, schema: str, table: str):
    import pandas as pd

    from airflow.providers.postgres.hooks import postgres

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    reader = pd.read_csv(
        url,
        chunksize=10000,
        encoding="utf-8",
        compression="zip",
        dtype=str,
    )

    with pg_hook.get_sqlalchemy_engine().begin() as conn:
        for i, df_chunk in enumerate(reader):
            df_chunk.to_sql(
                schema=schema,
                name=table,
                con=conn,
                if_exists="replace" if i == 0 else "append",
                index=False,
            )


# This dag should run during the night
# Based on the historic of publications on data.gouv.fr,
# the data is usually updated in the first week of the month.
EVERY_MONTH_ON_THE_10TH_AT_10_30_PM = "30 22 10 * *"


@dag(
    schedule=EVERY_MONTH_ON_THE_10TH_AT_10_30_PM,
    max_active_tasks=1,
    **dags.common_args(),
)
def import_sirene():
    dbt_build_staging = dbt.dbt_task.override(
        task_id="dbt_build_staging",
    )(
        command="build",
        select="path:models/staging/sirene",
    )

    schema = "sirene"

    chain(
        tasks.create_schema(name=schema),
        [
            import_file.override(task_id=f"import_{table}")(
                url=url, schema=schema, table=table
            )
            for table, url in constants.FILES.items()
        ],
        dbt_build_staging,
    )


import_sirene()
