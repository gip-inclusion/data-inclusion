import io
import json
from pathlib import Path
from typing import Optional

import pandas as pd
from pyairtable import api as pyairtable

from data_inclusion.scripts.tasks import utils


def extract_data(src: str, token: str, **kwargs) -> dict[str, io.BytesIO]:
    # extract airtable's base_id and table_name from url
    # cf https://support.airtable.com/docs/understanding-airtable-ids
    base_id = src.split("/")[-3]
    table_name = src.split("/")[-2]

    table = pyairtable.Table(api_key=token, base_id=base_id, table_name=table_name)
    data = table.all()

    with io.StringIO() as buf:
        json.dump(data, buf)
        return {"data.json": io.BytesIO(buf.getvalue().encode())}


def read_data(path: Path) -> tuple[pd.DataFrame, Optional[pd.Series]]:
    df = utils.read_json(path)
    return df, pd.json_normalize(df["fields"])["ID"]
