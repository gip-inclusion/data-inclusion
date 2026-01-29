import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Literal

import click
import pendulum
import s3fs
import sentry_sdk

from data_inclusion.api import auth
from data_inclusion.api.config import settings
from data_inclusion.api.core import db
from data_inclusion.api.decoupage_administratif.commands import import_communes

logger = logging.getLogger(__name__)

sentry_sdk.init(
    dsn=settings.SENTRY_DSN,
    environment=settings.ENV,
)


@click.group()
@click.version_option()
@click.option("--verbose", "-v", count=True)
@click.pass_context
def cli(ctx: click.Context, verbose: int):
    """api management commands"""
    logging.basicConfig(level=[logging.INFO, logging.INFO, logging.DEBUG][verbose])

    if ctx.obj is None:
        ctx.obj = ctx.with_resource(db.SessionLocal())


@cli.command(name="generate-token")
@click.argument("email", type=click.STRING)
@click.option(
    "--admin",
    is_flag=True,
    show_default=True,
    default=False,
    help="Generate an admin token",
)
@click.option(
    "--allowed-origin",
    "allowed_origins",
    multiple=True,
    help="Add an allowed origin for widget tokens (can be used multiple times)",
)
def _generate_token_for_user(
    email: str,
    admin: bool,
    allowed_origins: tuple[str, ...],
):
    """Generate a token associated with the given email."""
    allowed_origins_list = list(allowed_origins) if allowed_origins else None
    click.echo(
        auth.create_access_token(
            subject=email, admin=admin, allowed_origins=allowed_origins_list
        )
    )


def get_path(value: str, version: Literal["v0", "v1"]) -> Path:
    """Get a valid local path to the target dataset."""

    if value is not None and Path(value).exists():
        return Path(value).absolute()

    s3fs_client = s3fs.S3FileSystem(
        endpoint_url=settings.AWS_ENDPOINT_URL,
        key=settings.AWS_ACCESS_KEY_ID,
        secret=settings.AWS_SECRET_ACCESS_KEY,
    )

    if value is None:
        value = str(Path(settings.DATALAKE_BUCKET_NAME) / "data" / "marts")
        value = sorted(s3fs_client.ls(value))[-1]  # latest day
        value = sorted(s3fs_client.ls(value))[-1]  # latest run
        value = str(Path(value) / version)

        logger.info(f"Using {value}")

    if value.startswith(settings.DATALAKE_BUCKET_NAME):
        if not s3fs_client.exists(value):
            raise ValueError(f"Path does not exist in S3: {value}")

        tmpdir = tempfile.TemporaryDirectory(delete=False)
        local_path = Path(tmpdir.name) / value
        s3fs_client.get(value, str(local_path), recursive=True)
        return local_path

    raise ValueError(
        f"""Path must be a local path or start with
        the bucket name: {settings.DATALAKE_BUCKET_NAME}"""
    )


@sentry_sdk.monitor(
    monitor_slug="load-inclusion-data",
    monitor_config={
        "schedule": {"type": "crontab", "value": "30 5-22 * * *"},
        "checkin_margin": 60,
        "max_runtime": 60,
        "failure_issue_threshold": 1,
        "recovery_threshold": 1,
        "timezone": "Europe/Paris",
    },
)
@cli.command(name="load-inclusion-data")
@click.option(
    "--version",
    type=click.Choice(["v0", "v1"]),
    default="v1",
    show_default=True,
    help="Dataset version to load",
)
@click.option(
    "--path",
    callback=lambda ctx, param, value: get_path(
        value, version=ctx.params.get("version", param.default)
    ),
)
@click.pass_obj
def _load_inclusion_data(db_session, path: Path, version: Literal["v0", "v1"]):
    if version == "v1":
        from data_inclusion.api.inclusion_data.v1.commands import load_inclusion_data
    elif version == "v0":
        from data_inclusion.api.inclusion_data.v0.commands import load_inclusion_data

    load_inclusion_data(db_session=db_session, path=path)

    # if the dataset has been downloaded from s3
    if tempfile.gettempdir() in path.parents:
        path.rmdir()


@sentry_sdk.monitor(
    monitor_slug="export-analytics",
    monitor_config={
        "schedule": {"type": "crontab", "value": "45 2 * * *"},
        "checkin_margin": 60,
        "max_runtime": 60,
        "failure_issue_threshold": 1,
        "recovery_threshold": 1,
        "timezone": "Europe/Paris",
    },
)
@cli.command(name="export-analytics")
def _export_analytics():
    # TODO(vmttn): make this code testable with local export
    output_filename = "analytics.dump"

    if (stack := os.environ.get("STACK")) is not None and stack.startswith("scalingo"):
        subprocess.run("dbclient-fetcher pgsql", shell=True, check=True)

    s3fs_client = s3fs.S3FileSystem()
    base_key = Path(settings.DATALAKE_BUCKET_NAME) / "data" / "api"
    key = str(
        base_key
        / pendulum.now().date().isoformat()
        / pendulum.now().isoformat()
        / output_filename
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpfile = Path(tmpdir) / output_filename

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
            "--table api__*_events_v1 "
            f"--file {tmpfile}"
        )

        logger.info(command)
        subprocess.run(command, shell=True, check=True)

        logger.info(f"Storing to {key}")
        s3fs_client.put_file(tmpfile, key)


@cli.command(name="import-communes")
def _import_communes():
    """Import the communes from the Decoupage Administratif API"""
    import_communes()


if __name__ == "__main__":
    cli()
