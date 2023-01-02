import io
import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from data_inclusion.scripts.tasks import utils

EMPLOIS_SOURCE_STR = "emplois-de-linclusion"

logger = logging.getLogger(__name__)


class EmploisClient:
    def __init__(self, url: str, token: str) -> None:
        self.url = url
        self.session = requests.Session()
        adapter = HTTPAdapter(
            max_retries=Retry(total=2, backoff_factor=120, status_forcelist=[429])
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({"Authorization": f"Token {token}"})

    def list_structures(self) -> list:
        next_url = self.url
        structures_data = []

        pbar = None

        while True:
            response = self.session.get(next_url)
            data = response.json()

            if pbar is None:
                pbar = tqdm(total=data["count"], initial=len(data["results"]))
            else:
                pbar.update(len(data["results"]))
            structures_data += data["results"]
            next_url = data["next"]
            if next_url is None:
                break

        if pbar is not None:
            pbar.close()

        return structures_data


def extract_data(src: str, token: str, **kwargs) -> dict[str, io.BytesIO]:
    client = EmploisClient(url=src, token=token)

    # raw structures
    data = client.list_structures()

    with io.StringIO() as buf:
        json.dump(data, buf)
        return {"data.json": io.BytesIO(buf.getvalue().encode())}


def read_data(path: Path) -> tuple[pd.DataFrame, Optional[pd.Series]]:
    df = utils.read_json(path)
    return df, df.id


def transform_data(input_df: pd.DataFrame) -> pd.DataFrame:
    raw_structures_df = utils.deserialize_df_data(input_df)

    utils.log_df_info(raw_structures_df)
    structures_df = transform_structure_dataframe(raw_structures_df)
    utils.log_df_info(structures_df)

    return utils.serialize_df_data(structures_df)


def transform_structure_dataframe(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df = input_df.replace("", None)

    # data exposed by les emplois should be serialized in the data.inclusion schema
    # so start from there
    output_df = input_df.copy(deep=True)

    # source
    output_df = output_df.assign(source=EMPLOIS_SOURCE_STR)

    return output_df
