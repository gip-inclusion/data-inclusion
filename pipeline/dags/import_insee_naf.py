import logging

import airflow
import pendulum
from airflow.operators import empty, python

from dags.virtualenvs import PYTHON_BIN_PATH

logger = logging.getLogger(__name__)

default_args = {}


def _import_dataset():
    import pandas as pd
    from airflow.providers.postgres.hooks import postgres

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    series_by_level_index = {
        level_index: pd.read_excel(
            f"https://www.insee.fr/fr/statistiques/fichier/2120875/naf2008_liste_n{level_index}.xls",  # noqa: E501
            header=2,
            dtype=str,
        )
        .set_index("Code")
        .squeeze("columns")
        for level_index in range(1, 6)
    }

    df = pd.read_excel(
        "https://www.insee.fr/fr/statistiques/fichier/2120875/naf2008_5_niveaux.xls",
        dtype=str,
    )

    df = df.assign(
        **{
            f"LABEL{level_index}": df.apply(
                lambda row: series[row[f"NIV{level_index}"]], axis="columns"
            )
            for level_index, series in series_by_level_index.items()
        }
    )

    df = df.rename(
        columns={
            "NIV1": "section_code",
            "NIV2": "division_code",
            "NIV3": "group_code",
            "NIV4": "class_code",
            "NIV5": "code",
            "LABEL1": "section_label",
            "LABEL2": "division_label",
            "LABEL3": "group_label",
            "LABEL4": "class_label",
            "LABEL5": "label",
        }
    )

    engine = pg_hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        df.to_sql(
            "code_naf",
            schema="insee",
            con=conn,
            if_exists="replace",
            index=False,
        )


with airflow.DAG(
    dag_id="import_insee_naf",
    start_date=pendulum.datetime(2022, 1, 1, tz="Europe/Paris"),
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    import_dataset = python.ExternalPythonOperator(
        task_id="import_insee_naf",
        python=str(PYTHON_BIN_PATH),
        python_callable=_import_dataset,
    )

    start >> import_dataset >> end
