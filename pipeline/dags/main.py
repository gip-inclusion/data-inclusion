import pendulum

import airflow
from airflow.operators import empty

from dag_utils import date, marts
from dag_utils.dbt import (
    dbt_operator_factory,
    get_intermediate_tasks,
    get_staging_tasks,
)
from dag_utils.notifications import notify_failure_args

with airflow.DAG(
    dag_id="main",
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    default_args=notify_failure_args(),
    schedule="@hourly",
    catchup=False,
    concurrency=4,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    dbt_seed = dbt_operator_factory(
        task_id="dbt_seed",
        command="seed",
    )

    dbt_create_udfs = dbt_operator_factory(
        task_id="dbt_create_udfs",
        command="run-operation create_udfs",
    )

    (
        start
        >> dbt_seed
        >> dbt_create_udfs
        >> get_staging_tasks()
        >> get_intermediate_tasks()
        >> marts.export_di_dataset_to_s3()
        >> end
    )
