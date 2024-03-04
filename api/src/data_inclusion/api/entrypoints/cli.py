import logging

import click

from data_inclusion.api import services
from data_inclusion.api.core import db, jwt

logger = logging.getLogger(__name__)


@click.group()
@click.version_option()
@click.option("--verbose", "-v", count=True)
def cli(verbose: int):
    "api management commands"
    logging.basicConfig(level=[logging.INFO, logging.INFO, logging.DEBUG][verbose])


@cli.command(name="generate_token")
@click.argument("email", type=click.STRING)
@click.option(
    "--admin",
    is_flag=True,
    show_default=True,
    default=False,
    help="Generate an admin token",
)
def generate_token_for_user(
    email: str,
    admin: bool,
):
    """Generate a token associated with the given email."""
    click.echo(jwt.create_access_token(subject=email, admin=admin))


@cli.command(name="notify_soliguide")
def notify_soliguide():
    with db.SessionLocal() as db_session:
        services.batch_forward_requests_to_soliguide(
            db_session=db_session,
            soliguide_api_client=services.SoliguideAPIClient(),
        )


if __name__ == "__main__":
    cli()
