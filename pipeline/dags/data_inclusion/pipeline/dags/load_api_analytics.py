import pendulum

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.utils.trigger_rule import TriggerRule

from data_inclusion.pipeline.common import dags, dbt, tasks


@task.external_python(
    python=tasks.PYTHON_BIN_PATH,
    retries=1,
    retry_delay=pendulum.duration(seconds=10),
)
def import_data():
    import subprocess
    import tempfile
    from pathlib import Path

    from airflow.models import Connection
    from airflow.providers.amazon.aws.fs import s3 as s3fs
    from airflow.providers.amazon.aws.hooks import s3

    pg_conn = Connection.get_connection_from_secrets(conn_id="pg")
    s3_hook = s3.S3Hook(aws_conn_id="s3")

    s3fs_client = s3fs.get_fs(conn_id="s3")

    BASE_KEY = Path(s3_hook.service_config["bucket_name"]) / "data" / "api"
    value = sorted(s3fs_client.ls(BASE_KEY))[-1]  # latest day
    value = sorted(s3fs_client.ls(value))[-1]  # latest run
    value = Path(value) / "analytics.dump"

    print(f"Using {value}")

    with tempfile.NamedTemporaryFile() as tmpfile:
        s3fs_client.get_file(rpath=value, lpath=tmpfile.name)

        command = (
            "pg_restore"
            f" --dbname={pg_conn.get_uri()}"
            " --clean"
            " --if-exists"
            " --no-owner"
            " --no-privileges"
            f" {tmpfile.name}"
        )

        try:
            print(command.replace(pg_conn.password, "***"))
            subprocess.run(command, shell=True, check=True, capture_output=True)
        except subprocess.CalledProcessError as exc:
            print(exc.stdout)
            print(exc.stderr)
            raise exc


# At 45 minutes past the hour, between 5am and 10pm
FIFTEEN_BEFORE_THE_HOUR = "45 5-22 * * *"


@dag(
    schedule=FIFTEEN_BEFORE_THE_HOUR,
    **dags.common_args(use_sentry=True),
)
def load_api_analytics():
    build_source_stats = dbt.dbt_task.override(
        task_id="generate_source_stats",
        trigger_rule=TriggerRule.ALL_DONE,
    )(
        command="build",
        select="path:models/intermediate/006_quality",
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


load_api_analytics()
