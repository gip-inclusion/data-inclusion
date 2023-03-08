import logging

import airflow
import pendulum
from airflow.models import Variable
from airflow.operators import bash, empty, python
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

default_args = {
    "env": {
        "DBT_PROFILES_DIR": Variable.get("DBT_PROJECT_DIR"),
        "POSTGRES_HOST": "{{ conn.pg.host }}",
        "POSTGRES_PORT": "{{ conn.pg.port }}",
        "POSTGRES_USER": "{{ conn.pg.login }}",
        "POSTGRES_PASSWORD": "{{ conn.pg.password }}",
        "POSTGRES_DB": "{{ conn.pg.schema }}",
    },
    "cwd": Variable.get("DBT_PROJECT_DIR"),
}


def _geocode():
    import sqlalchemy as sqla

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


with airflow.DAG(
    dag_id="main",
    start_date=pendulum.datetime(2022, 1, 1, tz="Europe/Paris"),
    default_args=default_args,
    schedule_interval="0 4 * * *",
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    dbt = "{{ var.value.pipx_bin }} run --spec dbt-postgres dbt"
    # this ensure deps are installed (if instance has been recreated)
    dbt = f"{dbt} deps && {dbt}"

    dbt_seed = bash.BashOperator(
        task_id="dbt_seed",
        bash_command=f"{dbt} seed --full-refresh",
    )

    # run what does not depend on geocoding results
    dbt_run_before_geocoding = bash.BashOperator(
        task_id="dbt_run_before_geocoding",
        bash_command=f"{dbt} run --exclude int_extra__geocoded_results+",
    )

    python_geocode = python.PythonOperator(
        task_id="python_geocode",
        python_callable=_geocode,
        pool="base_adresse_nationale_api",
    )

    # run remaining models
    dbt_run_after_geocoding = bash.BashOperator(
        task_id="dbt_run_after_geocoding",
        bash_command=f"{dbt} run --select int_extra__geocoded_results+",
    )

    dbt_test = bash.BashOperator(
        task_id="dbt_test",
        bash_command=f"{dbt} test",
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
