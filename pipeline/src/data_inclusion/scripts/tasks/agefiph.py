import json
from functools import reduce
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import trafilatura


def html_to_markdown(s: Optional[str]) -> Optional[str]:
    if s is None or s == "":
        return s
    return trafilatura.extract(trafilatura.load_html("<html>" + s + "</html>"))


def read(path: Path) -> pd.DataFrame:
    # utils.read_json is enough
    # but this adds the conversion of descriptions from html to markdown
    # should eventually be implemented as a python dbt model

    with path.open() as file:
        data = json.load(file)["data"]

    def _get(d: dict, keys: list[str]):
        return reduce(lambda subd, key: subd.get(key) if subd else None, keys, d)

    def _set(d: dict, keys: list[str], value) -> None:
        for key in keys[:-1]:
            if key not in d or not isinstance(d[key], dict):
                d[key] = {}
            d = d[key]
        d[keys[-1]] = value

    for service_data in data:
        for keys in [
            ["attributes", "field_essentiel_ph", "processed"],
            ["attributes", "field_essentiel_employeur", "processed"],
            ["attributes", "field_texte_brut_long"],
        ]:
            _set(service_data, keys, html_to_markdown(_get(service_data, keys)))

    df = pd.DataFrame.from_records(data)
    df = df.replace({np.nan: None})

    return df
