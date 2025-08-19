#!/usr/bin/env python

import os
import subprocess
import tempfile
import textwrap
from pathlib import Path

import dotenv
import pendulum
import s3fs

OUTPUT_FILE = "analytics.dump"
BASE_KEY = Path(os.environ["DATALAKE_BUCKET_NAME"]) / "data" / "api"

dotenv.load_dotenv(dotenv.find_dotenv())


def export():
    s3fs_client = s3fs.S3FileSystem()
    key = str(
        BASE_KEY
        / pendulum.now().date().isoformat()
        / pendulum.now().isoformat()
        / OUTPUT_FILE
    )

    tmpdir = tempfile.TemporaryDirectory()
    file = Path(tmpdir.name) / OUTPUT_FILE

    command = textwrap.dedent(f"""\
        pg_dump $DATABASE_URL
        --format=custom
        --clean
        --if-exists
        --no-owner
        --no-privileges
        --section=pre-data
        --section=data
        --table api__*_events
        --file {file}
        """).replace("\n", " ")

    print(command)
    subprocess.run(command, shell=True, check=True)

    print(f"Storing to {key}")
    s3fs_client.put_file(file, key)

    tmpdir.cleanup()


if __name__ == "__main__":
    stack = os.environ.get("STACK")

    if stack is not None and stack.startswith("scalingo"):
        subprocess.run("dbclient-fetcher pgsql", shell=True, check=True)

    export()
