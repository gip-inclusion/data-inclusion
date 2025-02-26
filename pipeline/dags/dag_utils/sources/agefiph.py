import json
from functools import reduce
from pathlib import Path

from . import utils


def read(path: Path):
    # utils.df_from_json is enough
    # but this adds the conversion of descriptions from html to markdown
    # should eventually be implemented as a python dbt model
    import pandas as pd

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
            _set(service_data, keys, utils.html_to_markdown(_get(service_data, keys)))

    df = pd.DataFrame.from_records(data)
    return utils.df_clear_nan(df)
