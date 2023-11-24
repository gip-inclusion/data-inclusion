import airflow
import pendulum
from airflow.operators import empty, python
from airflow.utils.task_group import TaskGroup
from dag_utils.dbt import dbt_operator_factory
from dag_utils.notifications import format_failure, notify_webhook
from dag_utils.settings import SOURCES_CONFIGS
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
    concurrency=4,
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

    dbt_staging_tasks_list = []
    for source_config in sorted(SOURCES_CONFIGS, key=lambda d: d["id"]):
        dbt_source_id = source_config["id"].replace("-", "_")

        with TaskGroup(group_id=source_config["id"]) as source_task_group:
            dbt_run_staging = dbt_operator_factory(
                task_id="dbt_run_staging",
                command="run",
                select=f"path:models/staging/sources/**/stg_{dbt_source_id}__*.sql",
            )

            dbt_test_staging = dbt_operator_factory(
                task_id="dbt_test_staging",
                command="test",
                select=f"path:models/staging/sources/**/stg_{dbt_source_id}__*.sql",
            )

            # TODO: add intermediate models here
            # this would require to refactor mediation numerique models

            (dbt_run_staging >> dbt_test_staging)

        dbt_staging_tasks_list += [source_task_group]

    dbt_build_before_geocoding = dbt_operator_factory(
        task_id="dbt_build_before_geocoding",
        command="build",
        select=" ".join(
            [
                # FIXME: handle odspep as other sources (add to dags/settings.py)
                "path:models/staging/sources/odspep",
                "path:models/intermediate/sources/**/*",
                "path:models/intermediate/int__union_adresses.sql",
                "path:models/intermediate/int__union_services.sql",
                "path:models/intermediate/int__union_structures.sql",
            ]
        ),
    )

    python_geocode = python.ExternalPythonOperator(
        task_id="python_geocode",
        python=str(PYTHON_BIN_PATH),
        python_callable=_geocode,
    )

    dbt_build_after_geocoding = dbt_operator_factory(
        task_id="dbt_build_after_geocoding",
        command="build",
        select=" ".join(
            [
                "path:models/intermediate/extra",
                "path:models/intermediate/int__deprecated_sirets.sql",
                "path:models/intermediate/int__plausible_personal_emails.sql",
                "path:models/intermediate/int__union_adresses__enhanced.sql+",
                "path:models/intermediate/int__union_services__enhanced.sql+",
                "path:models/intermediate/int__union_structures__enhanced.sql+",
                "marts",
            ]
        ),
    )

    dbt_build_flux = dbt_operator_factory(
        task_id="dbt_build_flux",
        command="build",
        select="flux",
    )

    (
        start
        >> dbt_seed
        >> dbt_create_udfs
        >> dbt_staging_tasks_list
        >> dbt_build_before_geocoding
        >> python_geocode
        >> dbt_build_after_geocoding
        >> dbt_build_flux
        >> end
    )
