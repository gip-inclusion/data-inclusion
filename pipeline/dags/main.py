import pendulum

import airflow
from airflow.operators import empty, python

from dag_utils import date, marts
from dag_utils.dbt import (
    dbt_operator_factory,
    get_after_geocoding_tasks,
    get_before_geocoding_tasks,
    get_staging_tasks,
)
from dag_utils.notifications import format_failure, notify_webhook
from dag_utils.virtualenvs import PYTHON_BIN_PATH

default_args = {
    "on_failure_callback": lambda context: notify_webhook(
        context, "mattermost", format_failure
    )
}


def _geocode():
    import logging

    import sqlalchemy as sqla

    from airflow.models import Variable
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from dag_utils import geocoding
    from dag_utils.sources import utils

    logger = logging.getLogger(__name__)

    pg_hook = PostgresHook(postgres_conn_id="pg")

    # 1. Retrieve input data
    input_df = pg_hook.get_pandas_df(
        sql="""
            SELECT
                _di_surrogate_id,
                adresse,
                code_postal,
                commune
            FROM public_intermediate.int__union_adresses;
        """
    )

    utils.log_df_info(input_df, logger=logger)

    geocoding_backend = geocoding.BaseAdresseNationaleBackend(
        base_url=Variable.get("BAN_API_URL")
    )

    # 2. Geocode
    output_df = geocoding_backend.geocode(input_df)

    utils.log_df_info(output_df, logger=logger)

    # 3. Write result back
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.connect() as conn:
        with conn.begin():
            output_df.to_sql(
                "extra__geocoded_results",
                schema="public",
                con=conn,
                if_exists="replace",
                index=False,
                dtype={
                    "latitude": sqla.Float,
                    "longitude": sqla.Float,
                    "result_score": sqla.Float,
                },
            )


with airflow.DAG(
    dag_id="main",
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    default_args=default_args,
    schedule="0 4 * * *",
    catchup=False,
    concurrency=4,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    dbt_seed = dbt_operator_factory(
        task_id="dbt_seed",
        command="seed",
    )

    dbt_create_udfs = dbt_operator_factory(
        task_id="dbt_create_udfs",
        command="run-operation create_udfs",
    )

    python_geocode = python.ExternalPythonOperator(
        task_id="python_geocode",
        python=str(PYTHON_BIN_PATH),
        python_callable=_geocode,
    )

    (
        start
        >> dbt_seed
        >> dbt_create_udfs
        >> get_staging_tasks()
        >> get_before_geocoding_tasks()
        >> python_geocode
        >> get_after_geocoding_tasks()
        >> marts.export_di_dataset_to_s3()
        >> end
    )
