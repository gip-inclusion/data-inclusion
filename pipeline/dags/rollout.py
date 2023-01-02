import airflow
import pendulum
from airflow.operators import empty
from airflow.providers.postgres.operators import postgres

from data_inclusion.scripts import settings

default_args = {}

with airflow.DAG(
    dag_id="rollout",
    start_date=pendulum.datetime(2022, 1, 1, tz=settings.TIME_ZONE),
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    rollout = postgres.PostgresOperator(
        task_id="rollout",
        postgres_conn_id="pg",
        sql="sql/rollout.sql",
    )

    start >> rollout >> end
