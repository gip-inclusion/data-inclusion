import functools
from contextlib import contextmanager

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


@functools.cache
def hook():
    return PostgresHook(postgres_conn_id="pg")


@contextmanager
def connect_begin():
    engine = hook().get_sqlalchemy_engine()
    with engine.connect() as conn:
        with conn.begin():
            yield conn


def create_schema(schema_name: str) -> None:
    with connect_begin() as conn:
        conn.execute(
            f"""\
            CREATE SCHEMA IF NOT EXISTS {schema_name};
            GRANT USAGE ON SCHEMA {schema_name} TO PUBLIC;
            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema_name}
            GRANT SELECT ON TABLES TO PUBLIC;"""
        )


def load_source_df(source_id: str, stream_id: str, df: pd.DataFrame) -> None:
    import sqlalchemy as sqla
    from sqlalchemy.dialects.postgresql import JSONB

    with connect_begin() as conn:
        schema_name = source_id.replace("-", "_")
        table_name = stream_id.replace("-", "_")

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
