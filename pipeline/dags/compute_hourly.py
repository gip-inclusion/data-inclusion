import pendulum

import airflow
from airflow.operators import empty

from dag_utils import date, marts
from dag_utils.dbt import (
    get_after_geocoding_tasks,
    get_before_geocoding_tasks,
    get_staging_tasks,
)
from dag_utils.notifications import format_failure, notify_webhook

default_args = {
    "on_failure_callback": lambda context: notify_webhook(
        context, "mattermost", format_failure
    )
}

with airflow.DAG(
    dag_id="compute_hourly",
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    default_args=default_args,
    schedule="@hourly",
    catchup=False,
    concurrency=4,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    (
        start
        >> get_staging_tasks(schedule="@hourly")
        >> get_before_geocoding_tasks()
        >> get_after_geocoding_tasks()
        >> marts.export_di_dataset_to_s3()
        >> end
    )
