import json
from pathlib import Path
from typing import Literal

from airflow import configuration
from airflow.decorators import task
from airflow.models import Variable

AIRFLOW_HOME = Path(configuration.get_airflow_home())
DBT_PROJECT_PATH = AIRFLOW_HOME / "dbt"

DBT_BIN_PATH = str(
    Path(configuration.get_airflow_home()) / "venvs" / "dbt" / "bin" / "dbt"
)


@task.bash(
    append_env=True,
    env={
        "POSTGRES_HOST": "{{ conn.pg.host }}",
        "POSTGRES_PORT": "{{ conn.pg.port }}",
        "POSTGRES_USER": "{{ conn.pg.login }}",
        "POSTGRES_PASSWORD": "{{ conn.pg.password }}",
        "POSTGRES_DB": "{{ conn.pg.schema }}",
    },
    cwd=str(DBT_PROJECT_PATH),
)
def dbt_task(
    command: Literal["build", "run", "test", "snapshot", "seed", "run-operation"],
    dbt_vars: dict | None = None,
    select: str | None = None,
    exclude: str | None = None,
    macro: str | None = None,
):
    args = []
    match command:
        case "run-operation":
            args.append(macro)
        case "build" | "run" | "test" | "snapshot":
            if select is not None:
                args += ["--select", select]
            if exclude is not None:
                args += ["--exclude", exclude]
            if dbt_vars is not None:
                args += ["--vars", json.dumps(dbt_vars)]

    # do not run unit tests in production
    if command in ["build", "test"] and Variable.get("ENVIRONMENT", None) == "prod":
        args += ["--exclude-resource-type", "unit_test"]

    return f"{DBT_BIN_PATH} {command} {' '.join(args)}"
