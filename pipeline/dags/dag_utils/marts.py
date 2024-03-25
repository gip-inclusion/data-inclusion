from airflow.operators import bash, python

from dag_utils.virtualenvs import PYTHON_BIN_PATH

TMP_FILE_PATH = "/tmp/out.dump"


def _export_to_s3(tmp_file_path, logical_date, run_id):
    import logging

    from airflow.providers.amazon.aws.hooks import s3

    from dag_utils import date

    logger = logging.getLogger(__name__)
    s3_file_path = f"data/marts/{date.local_date_str(logical_date)}/{run_id}/api.dump"

    logger.info("Uploading dump to s3_path=%s", s3_file_path)

    s3_hook = s3.S3Hook(aws_conn_id="s3")
    s3_hook.load_file(filename=tmp_file_path, key=s3_file_path)


def pg_dump_api():
    return bash.BashOperator(
        task_id="pg_dump_api",
        bash_command=(
            "pg_dump --format=custom "
            "--dbname={{ conn.pg.get_uri() }} "
            "--schema=public "
            "--table=service "
            "--table=structure "
            f"--file={TMP_FILE_PATH}"
        ),
    )


def export_to_s3():
    return python.ExternalPythonOperator(
        task_id="export_to_s3",
        python=str(PYTHON_BIN_PATH),
        python_callable=_export_to_s3,
        op_kwargs={"tmp_file_path": TMP_FILE_PATH},
    )
