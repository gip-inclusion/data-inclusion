from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

from data_inclusion.pipeline.common import dags, dbt, tasks


@task.external_python(
    python=tasks.PYTHON_BIN_PATH,
    retries=2,
)
def import_prenoms(schema: str):
    import pandas as pd

    from airflow.providers.postgres.hooks import postgres

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    url = (
        "https://www.insee.fr/fr/statistiques/fichier/8595130/prenoms-2024-nat_csv.zip"
    )

    df = pd.read_csv(url, sep=";")

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
