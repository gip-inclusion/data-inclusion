from airflow.sdk import chain, dag, task

from data_inclusion.pipeline.common import dags, dbt, tasks


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
    retries=1,
)
def import_prenoms(schema: str):
    import urllib.error

    import pandas as pd
    import pendulum

    from airflow.providers.postgres.hooks import postgres

    from data_inclusion.pipeline.dags.import_etat_civil import constants

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    year = pendulum.now().year - 1
    year_before = year - 1

    try:
        url = constants.INSEE_URL.format(year=year)
        print("👉 Using URL:", url)
        df = pd.read_parquet(url)
    except (FileNotFoundError, urllib.error.HTTPError):
        print("❌ Failed")
        url = constants.INSEE_URL.format(year=year_before)
        print("👉 Fallback to URL:", url)
        df = pd.read_parquet(url)

    with pg_hook.get_sqlalchemy_engine().begin() as conn:
        df.to_sql(
            schema=schema,
            name="prenoms",
            con=conn,
            if_exists="replace",
            index=False,
        )


@dag(
    schedule="@yearly",
    **dags.common_args(),
)
def import_etat_civil():
    dbt_build_staging = dbt.dbt_task.override(
        task_id="dbt_build_staging",
    )(
        command="build",
        select="path:models/staging/etat_civil",
    )

    schema = "etat_civil"

    chain(
        tasks.create_schema(name=schema),
        import_prenoms(schema=schema),
        dbt_build_staging,
    )


import_etat_civil()
