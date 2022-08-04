import logging
from datetime import date
from typing import Optional

import click
import pandas as pd
import requests

from data_inclusion.api import settings
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


def log_and_raise(resp: requests.Response, *args, **kwargs):
    try:
        resp.raise_for_status()
    except requests.HTTPError as err:
        breakpoint()
        logger.error(resp.json())
        raise err


class DataGouvClient:
    def __init__(
        self,
        base_url: str,
        dataset_id: str,
        api_key: Optional[str] = None,
    ):
        self.base_url = base_url + "/1"
        self.dataset_id = dataset_id
        self.session = requests.Session()
        self.session.hooks["response"] = [log_and_raise]
        if api_key is not None:
            self.session.headers.update({"X-API-KEY": api_key})

    def upload_dataset_resource(self, resource_id: str, filepath_str: str):
        self.session.post(
            f"{self.base_url}/datasets/{self.dataset_id}/resources/{resource_id}/upload/",
            files={"file": open(filepath_str, "rb")},
        )


@cli.command(name="update_datagouv_resources")
def update_datagouv_resources():
    if (
        settings.DATAGOUV_API_URL is None
        or settings.DATAGOUV_DI_DATASET_ID is None
        or settings.DATAGOUV_API_KEY is None
    ):
        logger.error(
            "DATAGOUV_API_URL, DATAGOUV_DI_DATASET_ID, DATAGOUV_API_KEY must be set"
        )
        raise SystemExit()

    base_filename_str = f"structures-inclusion-{date.today().strftime('%Y%m%d')}"
    output_dir_path_str = "."
    base_filepath_str = f"{output_dir_path_str}/{base_filename_str}"

    df = pd.DataFrame.from_records(data=[])  # TODO
    df.to_json(f"{base_filepath_str}.json", orient="records", force_ascii=False)
    df.to_csv(f"{base_filepath_str}.csv", index=False)
    df.to_excel(f"{base_filepath_str}.xlsx", index=False)

    datagouv_client = DataGouvClient(
        base_url=settings.DATAGOUV_API_URL,
        dataset_id=settings.DATAGOUV_DI_DATASET_ID,
        api_key=settings.DATAGOUV_API_KEY,
    )
    if settings.DATAGOUV_DI_JSON_STRUCTURE_RESOURCE_ID is not None:
        datagouv_client.upload_dataset_resource(
            resource_id=settings.DATAGOUV_DI_JSON_STRUCTURE_RESOURCE_ID,
            filepath_str=f"{base_filepath_str}.json",
        )
    if settings.DATAGOUV_DI_CSV_STRUCTURE_RESOURCE_ID is not None:
        datagouv_client.upload_dataset_resource(
            resource_id=settings.DATAGOUV_DI_CSV_STRUCTURE_RESOURCE_ID,
            filepath_str=f"{base_filepath_str}.csv",
        )
    if settings.DATAGOUV_DI_XLSX_STRUCTURE_RESOURCE_ID is not None:
        datagouv_client.upload_dataset_resource(
            resource_id=settings.DATAGOUV_DI_XLSX_STRUCTURE_RESOURCE_ID,
            filepath_str=f"{base_filepath_str}.xlsx",
        )


if __name__ == "__main__":
    cli()
