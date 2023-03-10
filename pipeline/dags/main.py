import logging
from typing import Optional

import airflow
import pendulum
from airflow.models import Variable
from airflow.operators import bash, empty, python
from virtualenvs import DBT_PYTHON_BIN_PATH, PYTHON_BIN_PATH

logger = logging.getLogger(__name__)

default_args = {}


def _geocode():
    import sqlalchemy as sqla
    from airflow.models import Variable
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from data_inclusion.scripts.tasks import geocoding, utils

    pg_hook = PostgresHook(postgres_conn_id="pg")

    # 1. Retrieve input data
    input_df = pg_hook.get_pandas_df(
        sql="""
            SELECT
                _di_surrogate_id,
                adresse,
                code_postal,
                commune
            FROM public_intermediate.int__structures;
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


def dbt_operator_factory(
    task_id: str,
    command: str,
    select: Optional[str] = None,
    exclude: Optional[str] = None,
) -> bash.BashOperator:
    """A basic factory for bash operators operating dbt commands."""

    dbt_args = command
    if select is not None:
        dbt_args += f" --select {exclude}"
    if exclude is not None:
        dbt_args += f" --exclude {exclude}"

    return bash.BashOperator(
        task_id=task_id,
        bash_command=f"{DBT_PYTHON_BIN_PATH.parent / 'dbt'} {dbt_args}",
        env={
            "DBT_PROFILES_DIR": Variable.get("DBT_PROJECT_DIR"),
            "POSTGRES_HOST": "{{ conn.pg.host }}",
            "POSTGRES_PORT": "{{ conn.pg.port }}",
            "POSTGRES_USER": "{{ conn.pg.login }}",
            "POSTGRES_PASSWORD": "{{ conn.pg.password }}",
            "POSTGRES_DB": "{{ conn.pg.schema }}",
        },
        cwd=Variable.get("DBT_PROJECT_DIR"),
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
        command="seed",
    )

    # run what does not depend on geocoding results
    dbt_run_before_geocoding = dbt_operator_factory(
        task_id="dbt_run_before_geocoding",
        command="run",
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
        select="int_extra__geocoded_results+",
    )

    dbt_test = dbt_operator_factory(
        task_id="dbt_test",
        command="test",
    )

    (
        start
        >> dbt_seed
        >> dbt_run_before_geocoding
        >> python_geocode
        >> dbt_run_after_geocoding
        >> dbt_test
        >> end
    )
