import pendulum

import airflow
from airflow.operators import empty, python
from airflow.utils.task_group import TaskGroup

from dag_utils import date, sources
from dag_utils.dbt import dbt_operator_factory
from dag_utils.notifications import format_failure, notify_webhook
from dag_utils.virtualenvs import PYTHON_BIN_PATH

default_args = {
    "on_failure_callback": lambda context: notify_webhook(
        context,
        conn_id="mattermost",
        format_fn=format_failure,
    )
}


def extract_from_source_to_datalake_bucket(source_id, stream_id, run_id, logical_date):
    import logging

    from dag_utils import s3, sources

    logger = logging.getLogger(__name__)
    source = sources.SOURCES_CONFIGS[source_id]
    stream = source["streams"][stream_id]
    url = stream["url"]

    logger.info("Fetching file from url=%s", url)

    s3_file_path = s3.source_file_path(
        source_id=source_id,
        filename=stream["filename"],
        run_id=run_id,
        logical_date=logical_date,
    )

    # FIXME(vperron) : Not a great fan of those "extractors" that accept ids
    # and tokens, but it's not my main focus atm.
    extract_fn = sources.get_extractor(source_id, stream_id)

    s3.store_content(
        path=s3_file_path,
        content=extract_fn(url=url, token=stream.get("token"), id=stream_id),
    )


def load_from_s3_to_data_warehouse(source_id, stream_id, run_id, logical_date):
    import logging

    import pandas as pd
    import sqlalchemy as sqla
    from sqlalchemy.dialects.postgresql import JSONB

    from dag_utils import pg, s3, sources

    logger = logging.getLogger(__name__)
    source = sources.SOURCES_CONFIGS[source_id]
    stream = source["streams"][stream_id]
    url = stream["url"]

    s3_file_path = s3.source_file_path(
        source_id=source_id,
        filename=stream["filename"],
        run_id=run_id,
        logical_date=logical_date,
    )

    # FIXME(vperron) : Re-load the file as a dataframe. This seems a bit unefficient.
    tmp_file_path = s3.download_file(s3_file_path)

    logger.info("Downloading file s3_path=%s tmp_path=%s", s3_file_path, tmp_file_path)

    read_fn = sources.get_reader(source_id, stream_id)
    df = read_fn(path=tmp_file_path)

    df = pd.DataFrame().assign(data=df.apply(lambda row: row.to_dict(), axis="columns"))
    df = df.assign(_di_batch_id=run_id)
    df = df.assign(_di_source_id=source_id)
    df = df.assign(_di_stream_id=stream_id)
    df = df.assign(_di_source_url=url)
    df = df.assign(_di_stream_s3_key=s3_file_path)
    df = df.assign(_di_logical_date=logical_date)

    schema_name = source_id.replace("-", "_")
    table_name = stream_id.replace("-", "_")

    pg.create_schema(schema_name)

    with pg.connect_begin() as conn:
        df.to_sql(
            f"{table_name}_tmp",
            con=conn,
            schema=schema_name,
            if_exists="replace",
            index=False,
            dtype={
                "data": JSONB,
                "_di_logical_date": sqla.Date,
            },
        )

        conn.execute(
            f"""\
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                data              JSONB,
                _di_batch_id      TEXT,
                _di_source_id     TEXT,
                _di_stream_id     TEXT,
                _di_source_url    TEXT,
                _di_stream_s3_key TEXT,
                _di_logical_date  DATE
            );
            TRUNCATE {schema_name}.{table_name};
            INSERT INTO {schema_name}.{table_name}
            SELECT * FROM {schema_name}.{table_name}_tmp;
            DROP TABLE {schema_name}.{table_name}_tmp;"""
        )


for source_id, source_config in sources.SOURCES_CONFIGS.items():
    model_name = source_id.replace("-", "_")
    dag_id = f"import_{model_name}"

    with airflow.DAG(
        dag_id=dag_id,
        start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
        default_args=default_args,
        schedule=source_config["schedule"],
        catchup=False,
        tags=["source"],
        user_defined_macros={"local_ds": date.local_date_str},
    ) as dag:
        start = empty.EmptyOperator(task_id="start")
        end = empty.EmptyOperator(task_id="end")

        dbt_snapshot_source = dbt_operator_factory(
            task_id="dbt_snapshot_source",
            command="snapshot",
            select=f"sources.{model_name}",
        )

        for stream_id in source_config["streams"]:
            with TaskGroup(group_id=stream_id) as stream_task_group:
                extract = python.ExternalPythonOperator(
                    task_id="extract",
                    python=str(PYTHON_BIN_PATH),
                    python_callable=extract_from_source_to_datalake_bucket,
                    retries=2,
                    op_kwargs={
                        "source_id": source_id,
                        "stream_id": stream_id,
                    },
                )
                load = python.ExternalPythonOperator(
                    task_id="load",
                    python=str(PYTHON_BIN_PATH),
                    python_callable=load_from_s3_to_data_warehouse,
                    op_kwargs={
                        "source_id": source_id,
                        "stream_id": stream_id,
                    },
                )

                start >> extract >> load

            # FIXME(vperron) : didn't Valentin say that snapshots aren't actually used ?
            if source_config["snapshot"]:
                stream_task_group >> dbt_snapshot_source >> end
            else:
                stream_task_group >> end

    globals()[dag_id] = dag
