import pendulum

import airflow
from airflow.operators import empty

from dag_utils import date, marts, notifications
from dag_utils.dbt import (
    get_intermediate_tasks,
    get_staging_tasks,
)

with airflow.DAG(
    dag_id="compute_hourly",
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    default_args=notifications.notify_failure_args(),
    schedule="@hourly",
    catchup=False,
    concurrency=4,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    (
        start
        >> get_staging_tasks(schedule="@hourly")
        >> get_intermediate_tasks()
        >> marts.export_di_dataset_to_s3()
        >> end
    )
