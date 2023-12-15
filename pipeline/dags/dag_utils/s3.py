import io

from airflow.providers.amazon.aws.hooks import s3

from . import date


def source_file_path(
    source_id: str, filename: str, run_id: str, logical_date: str
) -> str:
    return "/".join(
        [
            "data",
            "raw",
            date.local_date_str(logical_date),
            source_id,
            run_id,
            filename,
        ]
    )


def store_content(
    path: str,
    content: bytes,
):
    s3_hook = s3.S3Hook(aws_conn_id="s3")
    with io.BytesIO(content) as buf:
        s3_hook.load_file_obj(
            key=path,
            file_obj=buf,
            replace=True,
        )


def download_file(path: str) -> str:
    s3_hook = s3.S3Hook(aws_conn_id="s3")
    return s3_hook.download_file(key=path)
