import hashlib
import io
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pytz
import requests

from data_inclusion.scripts import settings

logger = logging.getLogger(__name__)


def log_df_info(df: pd.DataFrame, logger: logging.Logger = logger):
    buf = io.StringIO()
    df.info(buf=buf)
    for line in buf.getvalue().splitlines():
        logger.info(line, stacklevel=2)


def normalize_datetime(dt: datetime) -> datetime:
    return pytz.timezone(settings.TIME_ZONE).localize(dt).astimezone(pytz.UTC)


def hash(value: str) -> str:
    return hashlib.md5(value.encode(), usedforsecurity=False).hexdigest()


def read_json(path: Path) -> pd.DataFrame:
    # read data while preventing rough conversion of datatypes
    # e.g. siret/code_insee must not be converted to float
    df = pd.read_json(path, dtype=False)
    # use none for null values
    df = df.replace({np.nan: None})

    return df


def to_json(df: pd.DataFrame, path: Path):
    df.to_json(path, orient="records", force_ascii=False)


def read_csv(path: Path, sep: str) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        sep=sep,
        encoding_errors="replace",
        on_bad_lines="warn",
        dtype=str,
    )
    # use none for null values
    df = df.replace({np.nan: None})

    return df


def read_excel(path: Path, sheet_name: Optional[str | int] = 0) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet_name, dtype=str)
    # use none for null values
    df = df.replace({np.nan: None})

    return df


def deserialize_df_data(df: pd.DataFrame) -> pd.DataFrame:
    df = pd.json_normalize(df.data, max_level=0)
    df = df.replace({np.nan: None})
    return df


def serialize_df_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace({np.nan: None})
    df = df.apply(lambda row: json.dumps(row.to_dict()), axis="columns")
    df = pd.DataFrame().assign(data=df)
    return df


def extract_http_content(url: str, **kwargs) -> bytes:
    return requests.get(url).content
