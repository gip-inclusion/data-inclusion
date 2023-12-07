import logging

import airflow
import pendulum
from airflow.operators import empty, python

from dag_utils.date import TIME_ZONE
from dag_utils.virtualenvs import PYTHON_BIN_PATH

logger = logging.getLogger(__name__)

default_args = {}


def _import_dataset():
    import pandas as pd
    from airflow.models import Variable
    from airflow.providers.postgres.hooks import postgres

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    df = pd.read_csv(Variable.get("INSEE_FIRSTNAME_FILE_URL"), sep=";")

    engine = pg_hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        df.to_sql(
            "external_insee_fichier_prenoms",
            con=conn,
            if_exists="replace",
            index=False,
        )


with airflow.DAG(
    dag_id="import_insee_firstnames",
    start_date=pendulum.datetime(2022, 1, 1, tz=TIME_ZONE),
    default_args=default_args,
    schedule_interval="@once",
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
