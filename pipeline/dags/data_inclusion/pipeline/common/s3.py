from pathlib import Path
from typing import Literal

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
