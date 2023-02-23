import io
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from data_inclusion.scripts.tasks import utils


def extract_data(src: str, **kwargs) -> dict[str, io.BytesIO]:
    response = requests.get(src)
    return {"data.xlsx": io.BytesIO(response.content)}


def read_data(path: Path) -> tuple[pd.DataFrame, Optional[pd.Series]]:
    df = utils.read_excel(path, sheet_name="Structures")
    return df, df["ID Structure"]
