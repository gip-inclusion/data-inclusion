import logging
from pathlib import Path

import airflow
import pandas as pd
import pendulum
from airflow.models import DagRun, Variable
from airflow.operators import empty, python
from airflow.providers.amazon.aws.hooks import s3
from airflow.providers.postgres.hooks import postgres

logger = logging.getLogger(__name__)

default_args = {}

SOLIGUIDE_S3_KEY_PREFIX = Variable.get("SOLIGUIDE_S3_KEY_PREFIX")


def _import_dataset(
    run_id: str,
    dag_run: DagRun,
):
    from sqlalchemy.dialects.postgresql import JSONB

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")
    pg_engine = pg_hook.get_sqlalchemy_engine()
    s3_hook = s3.S3Hook(aws_conn_id="s3_sources")

    logical_date = pendulum.instance(
        dag_run.logical_date.astimezone(dag.timezone)
    ).date()

    with pg_engine.connect() as conn:
        # put dataset in schema (for control access)
        with conn.begin():
            conn.execute("DROP SCHEMA IF EXISTS soliguide CASCADE;")
            conn.execute("CREATE SCHEMA soliguide;")
            conn.execute("GRANT USAGE ON SCHEMA soliguide TO PUBLIC;")
            conn.execute(
                "ALTER DEFAULT PRIVILEGES IN SCHEMA soliguide"
                " GRANT SELECT ON TABLES TO PUBLIC;"
            )

            # iterate over source files
            for s3_key in s3_hook.list_keys(prefix=SOLIGUIDE_S3_KEY_PREFIX):
                # read in data
                tmp_filename = s3_hook.download_file(key=s3_key)
                df = pd.read_json(tmp_filename, dtype=False)

                # add metadata
                df = df.assign(batch_id=run_id)
                df = df.assign(logical_date=logical_date)

                # load to postgress
                df.to_sql(
                    Path(s3_key).stem,
                    con=conn,
                    schema="soliguide",
                    if_exists="replace",
                    index=False,
                    dtype={
                        "close": JSONB,
                        "entity": JSONB,
                        "location": JSONB,
                        "modalities": JSONB,
                        "newhours": JSONB,
                        "photos": JSONB,
                        "position": JSONB,
                        "publics": JSONB,
                        "services_all": JSONB,
                        "tempInfos": JSONB,
                    },
                )


with airflow.DAG(
    dag_id="import_soliguide",
    start_date=pendulum.datetime(2022, 1, 1, tz="Europe/Paris"),
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    import_dataset = python.PythonOperator(
        task_id="import",
        python_callable=_import_dataset,
    )

    start >> import_dataset >> end
