import json
from typing import Optional

from airflow.models import Variable
from airflow.operators import bash
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from dag_utils.sources import SOURCES_CONFIGS
from dag_utils.virtualenvs import DBT_PYTHON_BIN_PATH


def dbt_operator_factory(
    task_id: str,
    command: str,
    dbt_vars: Optional[dict] = None,
    select: Optional[str] = None,
    exclude: Optional[str] = None,
    trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
    exclude_unit_tests: bool = False,
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
    if exclude_unit_tests:
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


def get_staging_tasks():
    task_list = []

    for source_id in sorted(SOURCES_CONFIGS):
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

            dbt_build_intermediate_tmp = dbt_operator_factory(
                task_id="dbt_build_intermediate_tmp",
                command="build",
                select=int_selector,
                dbt_vars={"build_intermediate_tmp": True},
            )

            dbt_run_intermediate = dbt_operator_factory(
                task_id="dbt_run_intermediate",
                command="run",
                select=int_selector,
            )

            (
                dbt_run_staging
                >> dbt_test_staging
                >> dbt_build_intermediate_tmp
                >> dbt_run_intermediate
            )

        task_list += [source_task_group]

    return task_list


def get_intermediate_tasks():
    exclude_unit_tests = True if Variable.get("ENVIRONMENT", None) == "prod" else False
    return dbt_operator_factory(
        task_id="dbt_build_intermediate",
        command="build",
        select=" ".join(
            [
                # NOTE(vperron, 2024-09-02) : The contacts union is build here,
                # out of coherence but I don't really like that now the "brevo"
                # machinery depends on 3 DAGs to run correctly: import_brevo,
                # main and notify_rgpd_contacts. Maybe it should be concentrated
                # into a single DAG. Another way to see it is that it depended on
                # main since the beginning as it required intermediate data to be
                # present ?
                "path:models/intermediate/int__courriels_personnels.sql",
                "path:models/intermediate/int__criteres_qualite.sql",
                "path:models/intermediate/int__geocodages.sql",
                "path:models/intermediate/int__union_contacts.sql",
                "path:models/intermediate/int__union_adresses.sql",
                "path:models/intermediate/int__union_services.sql",
                "path:models/intermediate/int__union_structures.sql",
                "path:models/intermediate/int__union_contacts__enhanced.sql+",
                "path:models/intermediate/int__union_adresses__enhanced.sql+",
                "path:models/intermediate/int__union_services__enhanced.sql+",
                "path:models/intermediate/int__union_structures__enhanced.sql+",
                "path:models/intermediate/deduplicate/*",
                "marts",
            ]
        ),
        exclude=" ".join(
            [
                "path:models/intermediate/quality/int_quality__stats.sql+",
            ]
        ),
        trigger_rule=TriggerRule.ALL_DONE,
        exclude_unit_tests=exclude_unit_tests,
    )
