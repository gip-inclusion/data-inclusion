from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

from data_inclusion.pipeline.common import dags, dbt, tasks


@task.external_python(python=tasks.PYTHON_BIN_PATH)
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


EVERY_MONTH_ON_THE_10TH = "30 3 10 * *"

FILES = {
    "etablissement_historique": "https://www.data.gouv.fr/api/1/datasets/r/88fbb6b4-0320-443e-b739-b4376a012c32",
    "etablissement_succession": "https://www.data.gouv.fr/api/1/datasets/r/9c4d5d9c-4bbb-4b9c-837a-6155cb589e26",
    "stock_unite_legale": "https://www.data.gouv.fr/api/1/datasets/r/825f4199-cadd-486c-ac46-a65a8ea1a047",
    "stock_etablissement": "https://www.data.gouv.fr/api/1/datasets/r/0651fb76-bcf3-4f6a-a38d-bc04fa708576",
}


@dag(
    schedule=EVERY_MONTH_ON_THE_10TH,
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
            for table, url in FILES.items()
        ],
        dbt_build_staging,
    )


import_sirene()
