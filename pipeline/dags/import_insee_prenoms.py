import pendulum

from airflow.decorators import dag, task
from airflow.operators import empty

from dag_utils import date, dbt
from dag_utils.virtualenvs import PYTHON_BIN_PATH


@task.external_python(
    python=str(PYTHON_BIN_PATH),
    retries=2,
)
def extract_and_load():
    import pandas as pd

    from dag_utils import pg

    url = "https://www.insee.fr/fr/statistiques/fichier/2540004/nat2021_csv.zip"

    df = pd.read_csv(url, sep=";")

    schema = "insee"
    pg.create_schema(schema)

    with pg.connect_begin() as conn:
        df.to_sql(
            schema=schema,
            name="etat_civil_prenoms",
            con=conn,
            if_exists="replace",
            index=False,
        )


@dag(
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    schedule="@yearly",
    catchup=False,
)
def import_insee_prenoms():
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    dbt_build_staging = dbt.dbt_operator_factory(
        task_id="dbt_build_staging",
        command="build",
        select="path:models/staging/insee",
    )

    start >> extract_and_load() >> dbt_build_staging >> end


import_insee_prenoms()
