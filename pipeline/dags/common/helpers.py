from pathlib import Path


def s3_file_path(source_id: str) -> Path:
    """Generate a s3 file path to store the extracted raw data.

    The path should typically be extended with filenames.

    Returns:
        str: An airflow template string that should be provided
            as a task parameter (to be evaluated).
    """

    # airflow templated values
    logical_date = "{{ logical_date.in_timezone('Europe/Paris').date() }}"
    run_id = "{{ run_id }}"

    return Path() / "data" / "raw" / logical_date / source_id / run_id
