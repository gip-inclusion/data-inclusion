import io
import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from data_inclusion.scripts.tasks import utils

logger = logging.getLogger(__name__)


def log_and_raise(resp: requests.Response, *args, **kwargs):
    try:
        resp.raise_for_status()
    except requests.HTTPError as err:
        logger.error(resp.json())
        raise err


class UnJeuneUneSolutionClient:
    def __init__(self, url: str) -> None:
        self.url = url.rstrip("/")
        self.session = requests.Session()
        self.session.hooks["response"] = [log_and_raise]

    def list_benefits(self) -> list:
        response = self.session.get(f"{self.url}/benefits")
        return response.json()

    def list_institutions(self) -> list:
        response = self.session.get(f"{self.url}/institutions")
        return response.json()


def extract_data(src: str, **kwargs) -> dict[str, io.BytesIO]:
    un_jeune_une_solution_client = UnJeuneUneSolutionClient(url=src)

    ret = {}
    for resource_str in ["institutions", "benefits"]:
        data = getattr(un_jeune_une_solution_client, f"list_{resource_str}")()
        with io.StringIO() as buf:
            json.dump(data, buf)
            ret[f"{resource_str}.json"] = io.BytesIO(buf.getvalue().encode())

    return ret


def read_data(path: Path) -> tuple[pd.DataFrame, Optional[pd.Series]]:
    df = utils.read_json(path)
    return df, df.id
