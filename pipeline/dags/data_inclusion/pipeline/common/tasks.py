from pathlib import Path

from airflow import configuration
from airflow.decorators import task
from airflow.providers.postgres.hooks import postgres

PYTHON_BIN_PATH = str(
    Path(configuration.get_airflow_home()) / "venvs" / "tasks" / "bin" / "python"
)


@task.python
def create_schema(name: str):
    """Create a schema in the `pg` database and grant public access.

    Args:
        name (str): The name of the schema to create. Should be a valid schema name (no
            hyphens, etc.).
    """
    hook = postgres.PostgresHook(postgres_conn_id="pg")
    engine = hook.get_sqlalchemy_engine()
    with engine.begin() as conn:
        conn.execute(
            f"""\
            CREATE SCHEMA IF NOT EXISTS {name};
            GRANT USAGE ON SCHEMA {name} TO PUBLIC;
            ALTER DEFAULT PRIVILEGES IN SCHEMA {name}
            GRANT SELECT ON TABLES TO PUBLIC;"""
        )
