import logging

import airflow
import pendulum
from airflow.operators import empty, python

from dag_utils import date, sources
from dag_utils.dbt import dbt_operator_factory
from dag_utils.notifications import format_failure, notify_webhook
from dag_utils.virtualenvs import PYTHON_BIN_PATH

logger = logging.getLogger(__name__)


default_args = {
    "on_failure_callback": lambda context: notify_webhook(
        context,
        conn_id="mattermost",
        format_fn=format_failure,
    )
}


def extract_to_datalake_and_load_to_datawarehouse(
    source_id, stream_id, run_id, logical_date
):
    import pandas as pd
    import sqlalchemy as sqla
    from sqlalchemy.dialects.postgresql import JSONB

    from dag_utils import pg, s3, sources

    source = sources.load(source_id)
    stream = source.stream(stream_id)

    print(">>>>>>>>>>>>>> url", source.url, stream.url)

    s3_file_path = s3.source_file_path(
        source_id=source_id,
        filename=stream.filename,
        run_id=run_id,
        logical_date=logical_date,
    )

    s3.store_content(
        path=s3_file_path,
        content=stream.extract_fn(),
    )

    # FIXME(vperron) : Re-load the file as a dataframe. This seems a bit unefficient.
    tmp_file_path = s3.download_file(s3_file_path)

    print(">>>>>>>>>>>>>> DL FILE", tmp_file_path)

    df = stream.load_fn(path=tmp_file_path)

    df = pd.DataFrame().assign(data=df.apply(lambda row: row.to_dict(), axis="columns"))
    df = df.assign(_di_batch_id=run_id)
    df = df.assign(_di_source_id=source_id)
    df = df.assign(_di_stream_id=stream_id)
    df = df.assign(_di_source_url=stream.url or source.url)
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


for source in sources.SOURCES_CONFIGS:
    model_name = source.id.replace("-", "_")
    dag_id = f"import_{model_name}"

    with airflow.DAG(
        dag_id=dag_id,
        start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
        default_args=default_args,
        schedule=source.schedule,
        catchup=False,
        tags=["source"],
    ) as dag:
        start = empty.EmptyOperator(task_id="start")
        end = empty.EmptyOperator(task_id="end")

        extract_load_tasks = [
            python.ExternalPythonOperator(
                task_id=f"extract_load_{stream.id}",
                python=str(PYTHON_BIN_PATH),
                python_callable=extract_to_datalake_and_load_to_datawarehouse,
                op_kwargs={
                    "source_id": source.id,
                    "stream_id": stream.id,
                },
            )
            for stream in source.streams
        ]

        # FIXME(vperron) : we have zero tests defined on any source
        dbt_test_source = dbt_operator_factory(
            task_id="dbt_test_source",
            command="test",
            select=f"source:{model_name}",
        )

        # FIXME(vperron) : didn't Valentin say that snapshots aren't actually used ?
        if source.should_snapshot:
            dbt_snapshot_source = dbt_operator_factory(
                task_id="dbt_snapshot_source",
                command="snapshot",
                select=model_name,
            )
            dbt_test_source >> dbt_snapshot_source >> end
        else:
            dbt_test_source >> end

        start >> extract_load_tasks >> dbt_test_source

    globals()[dag_id] = dag
