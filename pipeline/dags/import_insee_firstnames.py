import logging

import pendulum

import airflow
from airflow.operators import empty, python

from dag_utils import date
from dag_utils.virtualenvs import PYTHON_BIN_PATH

logger = logging.getLogger(__name__)

default_args = {}


def _import_dataset():
    import pandas as pd

    from airflow.models import Variable

    from dag_utils import pg

    df = pd.read_csv(Variable.get("INSEE_FIRSTNAME_FILE_URL"), sep=";")

    with pg.connect_begin() as conn:
        df.to_sql(
            "external_insee_fichier_prenoms",
            # FIXME(vperron): the schema should be "insee" instead of "public"
            con=conn,
            if_exists="replace",
            index=False,
        )


with airflow.DAG(
    dag_id="import_insee_firstnames",
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    default_args=default_args,
    schedule="@once",
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    import_dataset = python.ExternalPythonOperator(
        task_id="import",
        python=str(PYTHON_BIN_PATH),
        python_callable=_import_dataset,
    )

    start >> import_dataset >> end
