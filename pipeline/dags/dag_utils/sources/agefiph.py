import json
from functools import reduce
from pathlib import Path

from . import utils


class APIClient(utils.BaseApiClient):
    def __init__(self, base_url: str):
        super().__init__(base_url)

    def get_structures(self) -> list[dict]:
        url = self.base_url
        result = []
        i = 0
        while True:
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()
            result += data["data"]
            if "next" not in data["links"]:
                break
            if i > 15:
                raise RuntimeError("explain why too many iterations here")
            url = data["links"]["next"]["href"]
            i += 1
        return result


def read(path: Path):
    # utils.df_from_json is enough
    # but this adds the conversion of descriptions from html to markdown
    # should eventually be implemented as a python dbt model
    import pandas as pd

    with path.open() as file:
        data = json.load(file)

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
            ["attributes", "field_solution_detail", "processed"],
        ]:
            _set(service_data, keys, utils.html_to_markdown(_get(service_data, keys)))

    df = pd.DataFrame.from_records(data)
    return utils.df_clear_nan(df)


def extract(url: str, **kwargs) -> bytes:
    print(f"Fetching data from {url}")
    agefiph_client = APIClient(base_url=url)
    data = agefiph_client.get_structures()
    return json.dumps(data).encode()
