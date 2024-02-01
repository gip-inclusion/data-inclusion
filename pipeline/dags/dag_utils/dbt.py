from typing import Optional

from airflow.models import Variable
from airflow.operators import bash
from airflow.utils.task_group import TaskGroup

from dag_utils.sources import SOURCES_CONFIGS
from dag_utils.virtualenvs import DBT_PYTHON_BIN_PATH


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


def get_staging_tasks(schedule=None):
    task_list = []

    for source_id, src_meta in sorted(SOURCES_CONFIGS.items()):
        if schedule and src_meta["schedule"] != schedule:
            continue

        dbt_source_id = source_id.replace("-", "_")

        stg_selector = f"path:models/staging/sources/**/stg_{dbt_source_id}__*.sql"
        int_selector = f"path:models/intermediate/sources/**/int_{dbt_source_id}__*.sql"

        with TaskGroup(group_id=source_id) as source_task_group:
            dbt_run_staging = dbt_operator_factory(
                task_id="dbt_run_staging",
                command="run",
                select=stg_selector,
            )

            dbt_test_staging = dbt_operator_factory(
                task_id="dbt_test_staging",
                command="test",
                select=stg_selector,
            )

            dbt_run_intermediate = dbt_operator_factory(
                task_id="dbt_run_intermediate",
                command="run",
                select=int_selector,
            )

            dbt_test_intermediate = dbt_operator_factory(
                task_id="dbt_test_intermediate",
                command="test",
                select=int_selector,
            )

            (
                dbt_run_staging
                >> dbt_test_staging
                >> dbt_run_intermediate
                >> dbt_test_intermediate
            )

        task_list += [source_task_group]

    return task_list


def get_before_geocoding_tasks():
    return dbt_operator_factory(
        task_id="dbt_build_before_geocoding",
        command="build",
        select=" ".join(
            [
                # FIXME: handle odspep as other sources (add to dags/settings.py)
                "path:models/staging/sources/odspep",
                "path:models/intermediate/int__union_adresses.sql",
                "path:models/intermediate/int__union_services.sql",
                "path:models/intermediate/int__union_structures.sql",
            ]
        ),
    )


def get_after_geocoding_tasks():
    return dbt_operator_factory(
        task_id="dbt_build_after_geocoding",
        command="build",
        select=" ".join(
            [
                "path:models/intermediate/extra",
                "path:models/intermediate/int__deprecated_sirets.sql",
                "path:models/intermediate/int__plausible_personal_emails.sql",
                "path:models/intermediate/int__union_adresses__enhanced.sql+",
                "path:models/intermediate/int__union_services__enhanced.sql+",
                "path:models/intermediate/int__union_structures__enhanced.sql+",
                "marts",
            ]
        ),
    )
