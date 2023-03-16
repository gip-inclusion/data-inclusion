import logging
from typing import Optional

import airflow
import pendulum
from airflow.models import Variable
from airflow.operators import bash, empty, python
from airflow.utils.task_group import TaskGroup

from dags.notifications import format_failure, notify_webhook
from dags.settings import SOURCES_CONFIGS
from dags.virtualenvs import DBT_PYTHON_BIN_PATH, PYTHON_BIN_PATH

logger = logging.getLogger(__name__)


default_args = {
    "on_failure_callback": lambda context: notify_webhook(
        context, "mattermost", format_failure
    )
}


def _setup(source_config: dict):
    """Ensure the db objects (schema, permissions) subsequently required exist."""

    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id="pg")
    pg_engine = pg_hook.get_sqlalchemy_engine()
    schema_name = source_config["id"].replace("-", "_")

    with pg_engine.connect() as conn:
        with conn.begin():
            conn.execute(
                f"""\
                CREATE SCHEMA IF NOT EXISTS {schema_name};
                GRANT USAGE ON SCHEMA {schema_name} TO PUBLIC;
                ALTER DEFAULT PRIVILEGES IN SCHEMA {schema_name}
                GRANT SELECT ON TABLES TO PUBLIC;"""
            )


def _extract(
    stream_config: dict,
    source_config: dict,
    run_id: str,
    logical_date,
):
    """Extract raw data from source and store it into datalake bucket."""

    import io

    import pendulum
    from airflow.providers.amazon.aws.hooks import s3

    from data_inclusion.scripts.tasks import (
        dora,
        emplois_de_linclusion,
        mediation_numerique,
        mes_aides,
        utils,
    )

    logical_date = pendulum.instance(
        logical_date.astimezone(pendulum.timezone("Europe/Paris"))
    ).date()

    EXTRACT_FN_BY_SOURCE_ID = {
        "annuaire-du-service-public": utils.extract_http_content,
        "cd35": utils.extract_http_content,
        "cd72": utils.extract_http_content,
        "dora": dora.extract,
        "emplois-de-linclusion": emplois_de_linclusion.extract,
        "finess": utils.extract_http_content,
        "mes-aides": mes_aides.extract,
        "siao": utils.extract_http_content,
        "un-jeune-une-solution": utils.extract_http_content,
    }

    if source_config["id"].startswith("mediation-numerique-"):
        extract_fn = mediation_numerique.extract
    else:
        extract_fn = EXTRACT_FN_BY_SOURCE_ID[source_config["id"]]

    s3_hook = s3.S3Hook(aws_conn_id="s3")

    stream_s3_key = "/".join(
        [
            "data",
            "raw",
            logical_date.to_date_string(),
            source_config["id"],
            run_id,
            stream_config["filename"],
        ]
    )

    with io.BytesIO(extract_fn(**stream_config)) as buf:
        s3_hook.load_file_obj(
            file_obj=buf,
            key=stream_s3_key,
            replace=True,
        )


def _load(
    stream_config: dict,
    source_config: dict,
    run_id: str,
    logical_date,
):
    """Pull raw data from datalake bucket and load it with metadata to postgres."""

    from pathlib import Path

    import pandas as pd
    import pendulum
    import sqlalchemy as sqla
    from airflow.providers.amazon.aws.hooks import s3
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from sqlalchemy.dialects.postgresql import JSONB

    from data_inclusion.scripts.tasks import annuaire_du_service_public, utils

    READ_FN_BY_SOURCE_ID = {
        "annuaire-du-service-public": annuaire_du_service_public.read,
        "cd35": lambda path: utils.read_csv(path, sep=";"),
        "cd72": lambda path: utils.read_excel(path, sheet_name="Structures"),
        "dora": utils.read_json,
        "emplois-de-linclusion": utils.read_json,
        "finess": lambda path: utils.read_csv(path, sep=","),
        "mes-aides": utils.read_json,
        "siao": utils.read_excel,
        "un-jeune-une-solution": utils.read_json,
    }

    if source_config["id"].startswith("mediation-numerique-"):
        read_fn = utils.read_json
    else:
        read_fn = READ_FN_BY_SOURCE_ID[source_config["id"]]

    s3_hook = s3.S3Hook(aws_conn_id="s3")
    pg_hook = PostgresHook(postgres_conn_id="pg")
    pg_engine = pg_hook.get_sqlalchemy_engine()

    logical_date = pendulum.instance(
        logical_date.astimezone(pendulum.timezone("Europe/Paris"))
    ).date()

    stream_s3_key = "/".join(
        [
            "data",
            "raw",
            logical_date.to_date_string(),
            source_config["id"],
            run_id,
            stream_config["filename"],
        ]
    )

    tmp_filename = s3_hook.download_file(key=stream_s3_key)

    # read in data
    df = read_fn(path=Path(tmp_filename))

    # add metadata
    df = pd.DataFrame().assign(data=df.apply(lambda row: row.to_dict(), axis="columns"))
    df = df.assign(_di_batch_id=run_id)
    df = df.assign(_di_source_id=source_config["id"])
    df = df.assign(_di_stream_id=stream_config["id"])
    df = df.assign(_di_source_url=stream_config["url"])
    df = df.assign(_di_stream_s3_key=stream_s3_key)
    df = df.assign(_di_logical_date=logical_date)

    # load to postgres
    with pg_engine.connect() as conn:
        with conn.begin():
            schema_name = source_config["id"].replace("-", "_")

            df.to_sql(
                stream_config["id"].replace("-", "_"),
                con=conn,
                schema=schema_name,
                if_exists="replace",
                index=False,
                dtype={
                    "data": JSONB,
                    "_di_logical_date": sqla.Date,
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
        dbt_args += f" --select {select}"
    if exclude is not None:
        dbt_args += f" --exclude {exclude}"

    return bash.BashOperator(
        task_id=task_id,
        bash_command=f"{DBT_PYTHON_BIN_PATH.parent / 'dbt'} {dbt_args}",
        append_env=True,
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


# generate a dedicated DAG for every configured sources
for source_config in SOURCES_CONFIGS:
    dag_id = f"import_{source_config['id']}".replace("-", "_")
    dag = airflow.DAG(
        dag_id=dag_id,
        start_date=pendulum.datetime(2022, 1, 1, tz="Europe/Paris"),
        default_args=default_args,
        schedule_interval=source_config["schedule_interval"],
        catchup=False,
        tags=["source"],
    )

    with dag:
        start = empty.EmptyOperator(task_id="start")
        end = empty.EmptyOperator(task_id="end")

        setup = python.ExternalPythonOperator(
            task_id="setup",
            python=str(PYTHON_BIN_PATH),
            python_callable=_setup,
            op_kwargs={"source_config": source_config},
        )

        # tests here can detect impacting changes on the source data
        # before anything happens to the previously existing data
        dbt_test_source = dbt_operator_factory(
            task_id="dbt_test_source",
            command="test",
            select="source:data_inclusion." + source_config["id"].replace("-", "_"),
        )

        # historization of the raw data, if that makes sense
        if source_config["snapshot"]:
            dbt_snapshot_source = dbt_operator_factory(
                task_id="dbt_snapshot_source",
                command="snapshot",
                select=source_config["id"].replace("-", "_"),
            )
        else:
            dbt_snapshot_source = None

        # create dedicated embranchments for the extract/load of every streams in source
        for stream_config in source_config["streams"]:
            with TaskGroup(group_id=stream_config["id"]) as stream_task_group:
                extract = python.ExternalPythonOperator(
                    task_id="extract",
                    python=str(PYTHON_BIN_PATH),
                    python_callable=_extract,
                    retries=2,
                    op_kwargs={
                        "stream_config": stream_config,
                        "source_config": source_config,
                    },
                )
                load = python.ExternalPythonOperator(
                    task_id="load",
                    python=str(PYTHON_BIN_PATH),
                    python_callable=_load,
                    op_kwargs={
                        "stream_config": stream_config,
                        "source_config": source_config,
                    },
                )

                start >> setup >> extract >> load

            stream_task_group >> dbt_test_source

            if dbt_snapshot_source is not None:
                dbt_test_source >> dbt_snapshot_source >> end
            else:
                dbt_test_source >> end

    globals()[dag_id] = dag
