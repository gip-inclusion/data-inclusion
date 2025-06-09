import logging

import click

from data_inclusion.api import auth
from data_inclusion.api.decoupage_administratif.commands import import_communes
from data_inclusion.api.inclusion_data.commands import load_inclusion_data

logger = logging.getLogger(__name__)


@click.group()
@click.version_option()
@click.option("--verbose", "-v", count=True)
def cli(verbose: int):
    "api management commands"
    logging.basicConfig(level=[logging.INFO, logging.INFO, logging.DEBUG][verbose])


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


@cli.command(name="load-inclusion-data")
def _load_inclusion_data():
    """Load the latest inclusion data"""
    click.echo(load_inclusion_data())


@cli.command(name="import-communes")
def _import_communes():
    """Import the communes from the Decoupage Administratif API"""
    click.echo(import_communes())


if __name__ == "__main__":
    cli()
