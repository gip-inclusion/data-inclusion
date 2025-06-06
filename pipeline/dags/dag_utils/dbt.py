import json
from typing import Optional

from airflow.models import Variable
from airflow.operators import bash
from airflow.utils.trigger_rule import TriggerRule

from dag_utils.virtualenvs import DBT_PYTHON_BIN_PATH


def dbt_operator_factory(
    task_id: str,
    command: str,
    dbt_vars: Optional[dict] = None,
    select: Optional[str] = None,
    exclude: Optional[str] = None,
    trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
) -> bash.BashOperator:
    """A basic factory for bash operators operating dbt commands."""

    dbt_args = command
    if select is not None:
        dbt_args += f" --select {select}"
    if exclude is not None:
        dbt_args += f" --exclude {exclude}"
    if dbt_vars is not None:
        dbt_vars = json.dumps(dbt_vars)
        dbt_args += f" --vars '{dbt_vars}'"
    if Variable.get("ENVIRONMENT", None) == "prod":
        dbt_args += " --exclude-resource-type unit_test"

    return bash.BashOperator(
        task_id=task_id,
        bash_command=f"{DBT_PYTHON_BIN_PATH.parent / 'dbt'} {dbt_args}",
        append_env=True,
        trigger_rule=trigger_rule,
        env={
            "DBT_PROFILES_DIR": Variable.get("DBT_PROJECT_DIR"),
            "DBT_TARGET_PATH": Variable.get("DBT_TARGET_PATH", "target"),
            "DBT_LOG_PATH": Variable.get("DBT_LOG_PATH", "logs"),
            "POSTGRES_HOST": "{{ conn.pg.host }}",
            "POSTGRES_PORT": "{{ conn.pg.port }}",
            "POSTGRES_USER": "{{ conn.pg.login }}",
            "POSTGRES_PASSWORD": "{{ conn.pg.password }}",
            "POSTGRES_DB": "{{ conn.pg.schema }}",
        },
        cwd=Variable.get("DBT_PROJECT_DIR"),
    )
