from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators import empty
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    "backup_datawarehouse_to_s3",
    default_args=default_args,
    description="Backup PostgreSQL datawarehouse and upload to S3",
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
):
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")
    backup_postgres = BashOperator(
        task_id="backup_postgres",
        bash_command=f"""
        pg_dump -h {Variable.get('DATAWAREHOUSE_DI_HOST')} \
                -U {Variable.get('DATAWAREHOUSE_DI_USER')} \
                -d {Variable.get('DATAWAREHOUSE_DI_DATABASE')} -F c -f /tmp/backup.dump
        """,
        env={"PGPASSWORD": f"{Variable.get('DATAWAREHOUSE_DI_PASSWORD')}"},
    )
    logical_date = "{{ ds }}"
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename="/tmp/backup.dump",
        dest_key=f"S3://{Variable.get('DATAWAREHOUSE_BUCKET_NAME')}/data/backups/datawarehouse/backup_{logical_date}.dump",
        aws_conn_id="s3",
        replace=True,
    )

    clean_up = BashOperator(
        task_id="cleanup",
        bash_command="rm /tmp/backup.dump",
    )

    (start >> backup_postgres >> upload_to_s3 >> clean_up >> end)
