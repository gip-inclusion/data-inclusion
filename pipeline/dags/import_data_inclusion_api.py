import pendulum

import airflow
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule

from dag_utils import date, sentry
from dag_utils.dbt import dbt_operator_factory
from dag_utils.virtualenvs import PYTHON_BIN_PATH


@task.external_python(
    python=str(PYTHON_BIN_PATH),
    retries=3,
    retry_delay=pendulum.duration(seconds=10),
)
def import_data_inclusion_api():
    raise NotImplementedError()


HOURLY_AT_FIFTEEN = "15 * * * *"

with airflow.DAG(
    dag_id="import_data_inclusion_api",
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    default_args=sentry.notify_failure_args(),
    schedule=HOURLY_AT_FIFTEEN,
    catchup=False,
) as dag:
    build_source_stats = dbt_operator_factory(
        task_id="generate_source_stats",
        command="build",
        select="path:models/intermediate/quality",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    snapshot_source_stats = dbt_operator_factory(
        task_id="snapshot_source_stats",
        command="snapshot",
        select="quality",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        import_data_inclusion_api()
        # Will generate the daily stats 24 times a day.
        # The same table will be generated, the snapshot won't
        # be triggered except on day boundaries and it's fast.
        # The alternative would be more complicated code.
        >> build_source_stats
        >> snapshot_source_stats
    )
