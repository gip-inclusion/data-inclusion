from airflow.sdk import dag, task

from data_inclusion.pipeline.common import dags


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
    retries=1,
)
def import_data():
    import json
    import os
    import subprocess
    import tempfile
    from pathlib import Path

    import s3fs

    # https://github.com/apache/airflow/issues/51422#issuecomment-4068304878
    # This bug is still active :'(
    # But it does not crash, it HANGS.
    # This is because we're using task.virtualenv
    # - In production it works because FabAuthManager loads the entire airflow
    #   vars and secrets.
    # - run_main_pipeline uses task.python which uses the airflow environment and
    #   also has access to vars and secrets.
    # - import_api_analytics uses task.virtualenv which does not have access to
    #   vars and secrets.
    pg_uri = os.environ["AIRFLOW_CONN_PG"]
    s3_conn = json.loads(os.environ["AIRFLOW_CONN_S3"])
    s3_extra = s3_conn["extra"]
    s3_bucket = s3_extra["service_config"]["s3"]["bucket_name"]

    s3fs_client = s3fs.S3FileSystem(
        endpoint_url=s3_extra["endpoint_url"],
        key=s3_extra["aws_access_key_id"],
        secret=s3_extra["aws_secret_access_key"],
    )

    BASE_KEY = Path(s3_bucket) / "data" / "api"
    value = sorted(s3fs_client.ls(BASE_KEY))[-1]  # latest day
    value = sorted(s3fs_client.ls(value))[-1]  # latest run
    value = Path(value) / "analytics.dump"

    print(f"Using {value}")

    with tempfile.NamedTemporaryFile() as tmpfile:
        s3fs_client.get_file(rpath=value, lpath=tmpfile.name)

        command = (
            "pg_restore"
            f" --dbname={pg_uri}"
            " --clean"
            " --if-exists"
            " --no-owner"
            " --no-privileges"
            f" {tmpfile.name}"
        )

        try:
            from urllib.parse import urlparse

            parsed = urlparse(pg_uri)
            print(command.replace(parsed.password or "", "***"))
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
def import_api_analytics():
    import_data()


import_api_analytics()
