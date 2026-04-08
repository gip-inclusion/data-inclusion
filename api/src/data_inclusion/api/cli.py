import logging
import os
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import Literal

import click
import pandas as pd
import pendulum
import s3fs
import sentry_sdk
import sqlalchemy as sqla

from data_inclusion.api import auth
from data_inclusion.api.analytics.v1.models import ANALYTICS_EVENTS_TABLES_V1
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
    "--scope",
    "scopes",
    multiple=True,
    help="Add a scope to the token (can be used multiple times, defaults to 'api')",
)
@click.option(
    "--allowed-host",
    "allowed_hosts",
    multiple=True,
    help="Add an allowed host for widget tokens (can be used multiple times)",
)
def _generate_token_for_user(
    email: str,
    scopes: list[str],
    allowed_hosts: list[str],
):
    """Generate a token associated with the given email."""
    scopes_list = list(scopes) if scopes else None
    allowed_hosts_list = list(allowed_hosts) if allowed_hosts else None
    click.echo(
        auth.create_access_token(
            subject=email, scopes=scopes_list, allowed_hosts=allowed_hosts_list
        )
    )


def get_path(value: str | None, version: Literal["v0", "v1"]) -> Path:
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
        from data_inclusion.api.inclusion_data.v1.commands import load

        load(db_session=db_session, path=path)
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
    if (stack := os.environ.get("STACK")) is not None and stack.startswith("scalingo"):
        subprocess.run("dbclient-fetcher pgsql", shell=True, check=True)

    engine = sqla.create_engine(settings.DATABASE_URL)
    s3fs_client = s3fs.S3FileSystem()

    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        for table in ANALYTICS_EVENTS_TABLES_V1:
            df = pd.read_sql_table(table, engine)
            df["id"] = df["id"].astype(str).where(df["id"].notna())
            df.to_parquet(f"{table}.parquet", index=False)
            logger.info(f"Exported {table}")

        archive_filename = "analytics.tar.gz"
        with tarfile.open(archive_filename, "w:gz") as tar:
            for table in ANALYTICS_EVENTS_TABLES_V1:
                tar.add(f"{table}.parquet")

        key = str(
            Path(settings.DATALAKE_BUCKET_NAME)
            / "data"
            / "api"
            / pendulum.now().date().isoformat()
            / pendulum.now().isoformat()
            / archive_filename
        )
        logger.info(f"Storing to {key}")
        s3fs_client.put_file(str(archive_filename), key)


@cli.command(name="import-communes")
def _import_communes():
    """Import the communes from the Decoupage Administratif API"""
    import_communes()


if __name__ == "__main__":
    cli()


@sentry_sdk.monitor(
    monitor_slug="truncate-analytics",
    monitor_config={
        "schedule": {"type": "crontab", "value": "0 0 * * *"},
        "checkin_margin": 60,
        "max_runtime": 60,
        "failure_issue_threshold": 1,
        "recovery_threshold": 1,
        "timezone": "Europe/Paris",
    },
)
@cli.command(name="truncate-analytics")
def _truncate_analytics():
    for table in ANALYTICS_EVENTS_TABLES_V1:
        with db.SessionLocal() as db_session:
            rows_deleted = db_session.execute(
                sqla.text(
                    f"DELETE FROM {table} "
                    "WHERE created_at < NOW() - INTERVAL '13 months';"
                )
            )
            logger.info(f"Deleted {rows_deleted.rowcount} rows from {table}")
            db_session.commit()
