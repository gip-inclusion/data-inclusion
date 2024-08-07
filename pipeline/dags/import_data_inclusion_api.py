import pendulum

import airflow
from airflow.decorators import task
from airflow.operators import empty

from dag_utils import date
from dag_utils.virtualenvs import PYTHON_BIN_PATH

default_args = {}


@task.external_python(python=str(PYTHON_BIN_PATH))
def import_data_inclusion_api():
    import subprocess
    import tempfile

    from airflow.models import Connection
    from airflow.providers.ssh.hooks import ssh

    ssh_hook = ssh.SSHHook(ssh_conn_id="ssh_api")
    pg_dwh_conn = Connection.get_connection_from_secrets(conn_id="pg")
    pg_api_conn = Connection.get_connection_from_secrets(conn_id="pg_api")

    print("Tunnel creation...")

    with ssh_hook.get_tunnel(
        remote_host=pg_api_conn.host,
        remote_port=pg_api_conn.port,
    ) as tunnel:
        print("Tunnel created")

        pg_api_conn.host = tunnel.local_bind_host
        pg_api_conn.port = tunnel.local_bind_port
        pg_api_conn.conn_type = "postgresql"

        pg_api_conn.test_connection()

        API_DB_URL = pg_api_conn.get_uri()
        DWH_DB_URL = pg_dwh_conn.get_uri()

        with tempfile.NamedTemporaryFile() as tmp_file:
            try:
                command = (
                    f"pg_dump {API_DB_URL}"
                    " --format=custom"
                    " --clean"
                    " --if-exists"
                    " --no-owner"
                    " --no-privileges"
                    " --table api__requests"
                    # services & structures have foreign keys towards communes
                    " --table api__communes"
                    " --table api__services"
                    " --table api__structures"
                    f" --file {tmp_file.name}",
                )
                print(command)
                subprocess.run(command, shell=True, check=True, capture_output=True)

                command = (
                    "pg_restore"
                    f" --dbname={DWH_DB_URL}"
                    " --clean"
                    " --if-exists"
                    " --no-owner"
                    " --no-privileges"
                    f" {tmp_file.name}"
                )
                print(command)
                subprocess.run(command, shell=True, check=True, capture_output=True)
            except subprocess.CalledProcessError as exc:
                print(exc.stdout)
                print(exc.stderr)
                raise exc


HOURLY_AT_FIFTEEN = "15 * * * *"

with airflow.DAG(
    dag_id="import_data_inclusion_api",
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    default_args=default_args,
    schedule=HOURLY_AT_FIFTEEN,
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    start >> import_data_inclusion_api() >> end
