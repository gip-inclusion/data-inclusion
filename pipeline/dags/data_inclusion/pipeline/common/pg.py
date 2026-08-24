import math

import pandas as pd
from sqlalchemy.dialects.postgresql import JSONB

from airflow.providers.postgres.hooks import postgres

from data_inclusion.pipeline.common.utils import df_clear_nan


def _sanitize_for_json(value):
    if isinstance(value, dict):
        return {k: _sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_for_json(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if value is None:
        return None
    if pd.isna(value):
        return None
    return value


def to_pg(
    hook: postgres.PostgresHook,
    df: pd.DataFrame,
    schema_name: str,
    table_name: str,
):
    """Load a DataFrame into a PostgreSQL jsonb column.

    Internally, it uses a temporary table to stage the data before
    inserting it into the final destination table.

    Args:
        hook: The PostgresHook to use for the connection.
        df: The DataFrame to load.
        schema_name: The name of the schema to load the data into. Must be an existing
            schema.
        table_name: The name of the table to load the data into.
    """

    fqtn = f"{schema_name}.{table_name}"

    df = df_clear_nan(df)
    df = pd.DataFrame().assign(
        data=df.apply(
            lambda row: _sanitize_for_json(row.to_dict()),
            axis="columns",
        )
    )

    with hook.get_sqlalchemy_engine().begin() as conn:
        # load data into temporary table
        df.to_sql(
            f"{table_name}_tmp",
            con=conn,
            schema=schema_name,
            if_exists="replace",
            index=False,
            dtype={"data": JSONB},
        )

    hook.run(
        [
            f"""
                CREATE TABLE IF NOT EXISTS {fqtn} (data JSONB);
                TRUNCATE {fqtn};
                INSERT INTO {fqtn} SELECT * FROM {fqtn}_tmp;
                DROP TABLE {fqtn}_tmp;
            """
        ]
    )
