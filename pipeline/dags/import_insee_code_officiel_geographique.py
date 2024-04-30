import logging

import pendulum

import airflow
from airflow.operators import empty, python

from dag_utils import date
from dag_utils.virtualenvs import PYTHON_BIN_PATH

logger = logging.getLogger(__name__)

default_args = {}


def _import_dataset_ressource():
    from urllib.parse import urljoin

    import pandas as pd

    from airflow.models import Variable

    from dag_utils import pg

    base_url = Variable.get("INSEE_COG_DATASET_URL")

    pg.create_schema("insee")

    for resource in ["region", "departement", "commune"]:
        schema, table_name = "insee", f"{resource}s"
        url = urljoin(base_url, f"v_{resource}_2024.csv")

        print(f"Extracting {url}...")
        df = pd.read_csv(url, sep=",", dtype=str)

        print(f"Loading to {schema}.{table_name}")
        with pg.connect_begin() as conn:
            df.to_sql(
                schema=schema,
                name=f"{table_name}_tmp",
                con=conn,
                if_exists="replace",
                index=False,
            )

            conn.execute(
                f"""\
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (LIKE {schema}.{table_name}_tmp);
                TRUNCATE {schema}.{table_name};
                INSERT INTO {schema}.{table_name}
                SELECT * FROM {schema}.{table_name}_tmp;
                DROP TABLE {schema}.{table_name}_tmp;"""  # noqa: E501
            )


with airflow.DAG(
    dag_id="import_insee_code_officiel_geographique",
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    default_args=default_args,
    schedule="@once",
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    import_insee_dataset = python.ExternalPythonOperator(
        task_id="import_insee_dataset",
        python=str(PYTHON_BIN_PATH),
        python_callable=_import_dataset_ressource,
    )

    start >> import_insee_dataset >> end
