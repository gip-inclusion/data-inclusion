import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators import empty
from airflow.utils.dates import days_ago

from dag_utils import notifications
from dag_utils.virtualenvs import PYTHON_BIN_PATH


@task.external_python(
    python=str(PYTHON_BIN_PATH),
    retries=3,
    retry_delay=pendulum.duration(seconds=10),
)
def import_daily_dump_db():
    import subprocess
    import tempfile

    from airflow.models import Connection

    pg_dwh_conn = Connection.get_connection_from_secrets(conn_id="pg")

    DWH_DB_URL = pg_dwh_conn.get_uri()
    logical_date = "{{ ds }}"

    with tempfile.NamedTemporaryFile() as tmp_file:
        try:
            command = (
                f"pg_dump {DWH_DB_URL}"
                " --format=custom"
                " --clean"
                " --if-exists"
                " --no-owner"
                " --no-privileges"
                f" --file {tmp_file.name}",
            )
            print(command)
            subprocess.run(command, shell=True, check=True, capture_output=True)

            from dag_utils import s3

            s3.store_content(
                path=f"data/backups/datawarehouse/backup_{logical_date}.dump",
                content=tmp_file.read(),
            )
        except subprocess.CalledProcessError as exc:
            print(exc.stdout)
            print(exc.stderr)
            raise exc


DAILY_AT_TWO = "0 2 * * *"

with DAG(
    "backup_datawarehouse_to_s3",
    default_args=notifications.notify_failure_args(),
    description="Backup PostgreSQL datawarehouse and upload to S3",
    schedule_interval=DAILY_AT_TWO,
    start_date=days_ago(1),
    catchup=False,
):
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")
    (start >> import_daily_dump_db() >> end)
