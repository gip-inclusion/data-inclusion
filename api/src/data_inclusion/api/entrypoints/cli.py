import logging

import click
import numpy as np
import pandas as pd
import sqlalchemy as sqla

from data_inclusion.api import models, services
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


@cli.command(name="load_sources_metadata")
@click.argument("path_or_url", type=click.STRING)
def load_sources_metadata(path_or_url: str):
    df = pd.read_json(path_or_url, dtype=False)
    df = df.replace({np.nan: None})
    sources_dict_list = df.to_dict(orient="records")
    with db.SessionLocal() as db_session:
        with db_session.begin():
            db_session.execute(
                sqla.delete(models.Service).filter(
                    ~(
                        models.Service.source
                        == sqla.all_(
                            sqla.literal([d["slug"] for d in sources_dict_list])
                        )
                    )
                )
            )
            db_session.execute(
                sqla.delete(models.Structure).filter(
                    ~(
                        models.Structure.source
                        == sqla.all_(
                            sqla.literal([d["slug"] for d in sources_dict_list])
                        )
                    )
                )
            )
            db_session.execute(sqla.delete(models.Source))
            db_session.execute(sqla.insert(models.Source).values(sources_dict_list))


@cli.command(name="notify_soliguide")
def notify_soliguide():
    with db.SessionLocal() as db_session:
        services.batch_forward_requests_to_soliguide(
            db_session=db_session,
            soliguide_api_client=services.SoliguideAPIClient(),
        )


if __name__ == "__main__":
    cli()
