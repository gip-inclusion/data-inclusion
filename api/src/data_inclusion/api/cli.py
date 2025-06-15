import logging
import tempfile
from pathlib import Path

import click
import s3fs

from data_inclusion.api import auth
from data_inclusion.api.config import settings
from data_inclusion.api.core import db
from data_inclusion.api.decoupage_administratif.commands import import_communes
from data_inclusion.api.inclusion_data.commands import load_inclusion_data

logger = logging.getLogger(__name__)


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
def _generate_token_for_user(
    email: str,
    admin: bool,
):
    """Generate a token associated with the given email."""
    click.echo(auth.create_access_token(subject=email, admin=admin))


def get_path(value) -> Path:
    """Get a valid local path to the target dataset."""

    if value is not None and Path(value).exists():
        return Path(value).absolute()

    s3fs_client = s3fs.S3FileSystem()

    if value is None:
        value = str(Path(settings.DATALAKE_BUCKET_NAME) / "data" / "marts")
        value = sorted(s3fs_client.ls(value))[-1]  # latest day
        value = sorted(s3fs_client.ls(value))[-1]  # latest run

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


@cli.command(name="load-inclusion-data")
@click.option(
    "--path",
    callback=lambda ctx, param, value: get_path(value),
)
@click.pass_obj
def _load_inclusion_data(db_session, path: Path):
    load_inclusion_data(db_session=db_session, path=path)

    # if the dataset has been downloaded from s3
    if tempfile.gettempdir() in path.parents:
        path.rmdir()


@cli.command(name="import-communes")
def _import_communes():
    """Import the communes from the Decoupage Administratif API"""
    import_communes()


if __name__ == "__main__":
    cli()
