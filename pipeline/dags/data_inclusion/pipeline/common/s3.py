import io
import json
from pathlib import Path
from typing import Literal

from airflow.providers.amazon.aws.hooks import s3

# These are airflow templated values
# They will be evaluated at dag runtime
_LOGICAL_DATE_TEMPLATE = "{{ logical_date.in_timezone('Europe/Paris').date() }}"
_RUN_ID_TEMPLATE = "{{ run_id }}"


def get_key_for_raw_data(source_id: str) -> Path:
    return (
        Path() / "data" / "raw" / _LOGICAL_DATE_TEMPLATE / source_id / _RUN_ID_TEMPLATE
    )


def get_key_for_marts_data() -> Path:
    return Path() / "data" / "marts" / _LOGICAL_DATE_TEMPLATE / _RUN_ID_TEMPLATE


_FACTORIES = {"raw": get_key_for_raw_data, "marts": get_key_for_marts_data}


def get_key(
    stage: Literal["raw"] | Literal["marts"],
    **kwargs,
) -> Path:
    """Generate a s3 file path to store data.

    The path should typically be extended with filenames.

    Returns:
        str: An airflow template string that should be provided
            as a task parameter (to be evaluated).
    """
    return _FACTORIES[stage](**kwargs)


def to_s3(path: str, data: bytes | str | dict | list) -> str:
    """Store data to s3 under the given path."""

    s3_hook = s3.S3Hook(aws_conn_id="s3")

    if isinstance(data, (dict, list)):
        data = json.dumps(data)

    if isinstance(data, str):
        data = data.encode()

    with io.BytesIO(data) as buf:
        s3_hook.load_file_obj(
            key=path,
            file_obj=buf,
            replace=True,
        )

    return path


def from_s3(path: Path | str) -> Path:
    """Fetch data from s3 and store it in a local temporary file."""

    if isinstance(path, str):
        path = Path(path)

    s3_hook = s3.S3Hook(aws_conn_id="s3")

    return Path(s3_hook.download_file(key=str(path)))
