import pendulum

from airflow.decorators import dag, task

from dag_utils import date, dbt
from dag_utils.virtualenvs import PYTHON_BIN_PATH


@task.external_python(
    python=str(PYTHON_BIN_PATH),
    retries=2,
)
def import_prenoms():
    import pandas as pd

    from dag_utils import pg

    url = (
        "https://www.insee.fr/fr/statistiques/fichier/8595130/prenoms-2024-nat_csv.zip"
    )

    df = pd.read_csv(url, sep=";")

    schema = "etat_civil"
    pg.create_schema(schema)

    with pg.connect_begin() as conn:
        df.to_sql(
            schema=schema,
            name="prenoms",
            con=conn,
            if_exists="replace",
            index=False,
        )


@dag(
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    schedule="@yearly",
    catchup=False,
)
def import_etat_civil():
    dbt_build_staging = dbt.dbt_operator_factory(
        task_id="dbt_build_staging",
        command="build",
        select="path:models/staging/etat_civil",
    )

    import_prenoms() >> dbt_build_staging


import_etat_civil()
