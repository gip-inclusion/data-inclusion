import pendulum

from airflow.sdk import chain, dag, task
from airflow.utils.trigger_rule import TriggerRule

from data_inclusion.pipeline.common import dags, dbt, tasks


@task.external_python(
    python=tasks.PYTHON_BIN_PATH,
    retries=3,
    retry_delay=pendulum.duration(seconds=10),
)
def import_data():
    import subprocess
    import tempfile

    from airflow.providers.ssh.hooks import ssh
    from airflow.sdk import Connection

    ssh_hook = ssh.SSHHook(ssh_conn_id="ssh_api")
    pg_dwh_conn = Connection.get(conn_id="pg")
    pg_api_conn = Connection.get(conn_id="pg_api")

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
                    " --table api__*_events"
                    # services & structures have foreign keys towards communes
                    " --table api__communes"
                    " --table api__services"
                    " --table api__structures"
                    f" --file {tmp_file.name}"
                )
                print(command.replace(pg_api_conn.password, "***"))
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
                print(command.replace(pg_dwh_conn.password, "***"))
                subprocess.run(command, shell=True, check=True, capture_output=True)
            except subprocess.CalledProcessError as exc:
                print(exc.stdout)
                print(exc.stderr)
                raise exc


HOURLY_AT_FIFTEEN = "15 * * * *"


@dag(
    schedule=HOURLY_AT_FIFTEEN,
    **dags.common_args(use_sentry=True),
)
def import_data_inclusion_api():
    build_source_stats = dbt.dbt_task.override(
        task_id="generate_source_stats",
        trigger_rule=TriggerRule.ALL_DONE,
    )(
        command="build",
        select="path:models/intermediate/quality",
    )

    snapshot_source_stats = dbt.dbt_task.override(
        task_id="snapshot_source_stats",
        trigger_rule=TriggerRule.ALL_DONE,
    )(
        command="snapshot",
        select="quality",
    )

    chain(
        import_data()
        # Will generate the daily stats 24 times a day.
        # The same table will be generated, the snapshot won't
        # be triggered except on day boundaries and it's fast.
        # The alternative would be more complicated code.
        >> build_source_stats
        >> snapshot_source_stats
    )


import_data_inclusion_api()
