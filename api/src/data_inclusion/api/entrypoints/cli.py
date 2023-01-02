import logging

import click

from data_inclusion.api.core import jwt

logger = logging.getLogger(__name__)


@click.group()
@click.version_option()
@click.option("--verbose", "-v", count=True)
def cli(verbose: int):
    "api management commands"
    logging.basicConfig(level=[logging.INFO, logging.INFO, logging.DEBUG][verbose])


@cli.command(name="generate_token")
@click.argument("username", type=click.STRING)
def generate_token_for_user(
    username: str,
):
    """Generate a token associated with the given username."""
    click.echo(jwt.create_access_token(username))


if __name__ == "__main__":
    cli()
