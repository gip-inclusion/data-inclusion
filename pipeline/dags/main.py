import cosmos
import cosmos.airflow
import cosmos.constants
import cosmos.profiles
import pendulum

from airflow.models import Variable
from airflow.operators import empty

from dag_utils import date
from dag_utils.notifications import notify_failure_args
from dag_utils.virtualenvs import DBT_PYTHON_BIN_PATH

dag = cosmos.DbtDag(
    dag_id="main",
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    default_args=notify_failure_args(),
    schedule="0 4 * * *",
    catchup=False,
    project_config=cosmos.ProjectConfig(
        dbt_project_path=Variable.get("DBT_PROJECT_DIR"),
    ),
    profile_config=cosmos.ProfileConfig(
        profile_name="data_inclusion",
        target_name="dev",
        profile_mapping=cosmos.profiles.PostgresUserPasswordProfileMapping(
            conn_id="pg",
            profile_args={"schema": "public"},
        ),
    ),
    execution_config=cosmos.ExecutionConfig(
        dbt_executable_path=str(DBT_PYTHON_BIN_PATH.parent / "dbt")
    ),
    render_config=cosmos.RenderConfig(
        select=[
            "source:*",
            "path:models/staging/sources/**/*.sql",
            "path:models/intermediate/sources/**/*.sql",
            "path:models/intermediate/*.sql",
            "path:models/marts/**/*.sql",
        ],
        # show the source as start nodes in the graph
        node_converters={
            cosmos.constants.DbtResourceType("source"): lambda dag,
            task_group,
            node,
            **kwargs: empty.EmptyOperator(
                dag=dag, task_group=task_group, task_id=f"source_{node.name}"
            ),
        },
    ),
)
