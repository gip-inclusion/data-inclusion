import io
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from data_inclusion.scripts.tasks import utils


def extract_data(src: str, **kwargs) -> dict[str, io.BytesIO]:
    src = src.rstrip("/")

    return_dict = {}

    for file_name in [
        "ressources.csv",
        "contacts.csv",
        "horaires.csv",
        "familles.csv",
        "categories.csv",
        "sous_categories.csv",
    ]:
        response = requests.get(f"{src}/{file_name}")
        return_dict[file_name] = io.BytesIO(response.content)

    return return_dict


def read_data(path: Path) -> tuple[pd.DataFrame, Optional[pd.Series]]:
    df = utils.read_csv(path, sep="|")
    return df, None
