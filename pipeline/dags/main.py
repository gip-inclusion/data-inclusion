import airflow
import pendulum
from airflow.operators import empty, python

from dags.dbt import dbt_operator_factory
from dags.notifications import format_failure, notify_webhook
from dags.virtualenvs import PYTHON_BIN_PATH

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

    from data_inclusion.scripts.tasks import geocoding, utils

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
    start_date=pendulum.datetime(2022, 1, 1, tz="Europe/Paris"),
    default_args=default_args,
    schedule_interval="0 4 * * *",
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    dbt_seed = dbt_operator_factory(
        task_id="dbt_seed",
        command="seed --full-refresh",
    )

    dbt_create_udfs = dbt_operator_factory(
        task_id="dbt_create_udfs",
        command="run-operation create_udfs",
    )

    # run what does not depend on geocoding results
    dbt_run_before_geocoding = dbt_operator_factory(
        task_id="dbt_run_before_geocoding",
        command="run",
        select=" ".join(
            [
                "intermediate",
                "staging,tag:odspep",
            ]
        ),
        exclude="int_extra__geocoded_results+",
    )

    python_geocode = python.ExternalPythonOperator(
        task_id="python_geocode",
        python=str(PYTHON_BIN_PATH),
        python_callable=_geocode,
        pool="base_adresse_nationale_api",
    )

    # run remaining models
    dbt_run_after_geocoding = dbt_operator_factory(
        task_id="dbt_run_after_geocoding",
        command="run",
        select=" ".join(["intermediate,int_extra__geocoded_results+", "marts"]),
    )

    dbt_run_flux = dbt_operator_factory(
        task_id="dbt_run_flux",
        command="run",
        select="flux",
    )

    dbt_test = dbt_operator_factory(
        task_id="dbt_test",
        command="test",
    )

    (
        start
        >> dbt_seed
        >> dbt_create_udfs
        >> dbt_run_before_geocoding
        >> python_geocode
        >> dbt_run_after_geocoding
        >> dbt_run_flux
        >> dbt_test
        >> end
    )
