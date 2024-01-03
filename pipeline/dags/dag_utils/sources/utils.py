import io
import logging
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

logger = logging.getLogger(__name__)


def safe_urljoin(base, path):
    return urljoin(base, path) if base is not None else None


def log_df_info(df, logger: logging.Logger = logger):
    buf = io.StringIO()
    df.info(buf=buf)
    for line in buf.getvalue().splitlines():
        logger.info(line, stacklevel=2)


def read_json(path: Path):
    import numpy as np
    import pandas as pd

    # read data while preventing rough conversion of datatypes
    # e.g. siret/code_insee must not be converted to float
    df = pd.read_json(path, dtype=False)
    # use none for null values
    df = df.replace({np.nan: None})

    return df


def to_json(df, path: Path):
    df.to_json(path, orient="records", force_ascii=False)


def read_csv(path: Path, sep: str):
    import numpy as np
    import pandas as pd

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


def read_excel(path: Path, sheet_name: Optional[str | int] = 0):
    import numpy as np
    import pandas as pd

    df = pd.read_excel(path, sheet_name=sheet_name, dtype=str)
    # use none for null values
    df = df.replace({np.nan: None})

    return df


def extract_http_content(url: str, **kwargs) -> bytes:
    import requests

    response = requests.get(url)
    response.raise_for_status()
    return response.content
