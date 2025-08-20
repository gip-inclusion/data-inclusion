import os
import subprocess
import tempfile
from pathlib import Path

import pendulum
import s3fs

from data_inclusion.api.config import settings

OUTPUT_FILENAME = "analytics.dump"


def export_analytics():
    stack = os.environ.get("STACK")
    if stack is not None and stack.startswith("scalingo"):
        subprocess.run("dbclient-fetcher pgsql", shell=True, check=True)

    s3fs_client = s3fs.S3FileSystem()
    base_key = Path(settings.DATALAKE_BUCKET_NAME) / "data" / "api"
    key = str(
        base_key
        / pendulum.now().date().isoformat()
        / pendulum.now().isoformat()
        / OUTPUT_FILENAME
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpfile = Path(tmpdir) / OUTPUT_FILENAME

        command = (
            "pg_dump $DATABASE_URL "
            "--format=custom "
            "--clean "
            "--if-exists "
            "--no-owner "
            "--no-privileges "
            "--section=pre-data "
            "--section=data "
            "--table api__*_events "
            f"--file {tmpfile}"
        )

        print(command)
        subprocess.run(command, shell=True, check=True)

        print(f"Storing to {key}")
        s3fs_client.put_file(tmpfile, key)
