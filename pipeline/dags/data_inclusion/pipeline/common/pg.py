import pandas as pd
from sqlalchemy.dialects.postgresql import JSONB

from airflow.providers.postgres.hooks import postgres


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

    df = pd.DataFrame().assign(data=df.apply(lambda row: row.to_dict(), axis="columns"))

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
