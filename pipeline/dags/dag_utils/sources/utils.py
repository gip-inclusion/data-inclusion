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


def df_clear_nan(df):
    import numpy as np

    return df.replace({np.nan: None})


def df_from_json(path: Path):
    import pandas as pd

    # read data while preventing rough conversion of datatypes
    # e.g. siret/code_insee must not be converted to float
    df = pd.read_json(path, dtype=False)
    return df_clear_nan(df)


def to_json(df, path: Path):
    df.to_json(path, orient="records", force_ascii=False)


def read_csv(path: Path, sep: str):
    import pandas as pd

    df = pd.read_csv(
        path,
        sep=sep,
        encoding_errors="replace",
        on_bad_lines="warn",
        dtype=str,
    )
    return df_clear_nan(df)


def read_excel(path: Path, sheet_name: Optional[str | int] = 0):
    import pandas as pd

    df = pd.read_excel(path, sheet_name=sheet_name, dtype=str)
    return df_clear_nan(df)


def extract_http_content(url: str, **kwargs) -> bytes:
    import requests

    response = requests.get(url)
    response.raise_for_status()
    return response.content


def html_to_markdown(s: Optional[str]) -> Optional[str]:
    import trafilatura

    if s is None or s == "":
        return s
    return trafilatura.extract(trafilatura.load_html("<html>" + s + "</html>"))


def log_and_raise(resp, *args, **kwargs):
    import requests

    try:
        resp.raise_for_status()
    except requests.HTTPError as err:
        logger.error(resp.json())
        raise err


def logging_raising_session():
    import requests

    session = requests.Session()
    session.hooks["response"] = [log_and_raise]
    return session


def filename_from_url(path: list, param: dict) -> str:
    param_slug = [f"{k}-{v}" for k, v in sorted(param.items())]
    path = "-".join(path + param_slug)

    return f"{path}.json"


class BaseApiClient:
    def __init__(self, base_url: str, **kwargs):
        self.base_url = base_url.rstrip("/")
        self.session = logging_raising_session()
