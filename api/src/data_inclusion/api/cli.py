import logging

import click

from data_inclusion.api import auth

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
    click.echo(auth.create_access_token(subject=email, admin=admin))


if __name__ == "__main__":
    cli()
