import io
import json
import logging
import time
from copy import deepcopy
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests
import trafilatura
from lxml import html
from tqdm import tqdm

logger = logging.getLogger(__name__)


def log_and_raise(resp: requests.Response, *args, **kwargs):
    try:
        resp.raise_for_status()
    except requests.HTTPError as err:
        logger.error(resp.json())
        raise err


class APIClient:
    # Documentation on the soliguide API is available here:
    # https://apisolidarite.soliguide.fr/Documentation-technique-de-l-API-Solidarit-ecaf8198f0e9400d93140b8043c9f2ce

    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"JWT {token}"})
        self.session.hooks["response"] = [log_and_raise]

    def search(
        self,
        location_geo_type: str,
        location_geo_value: Optional[str] = None,
    ) -> list[dict]:
        if location_geo_type != "position" and location_geo_value is None:
            raise Exception("Missing location.geoValue.")

        default_data = {
            "location": {
                "geoType": location_geo_type,
            },
            "options": {
                # this value must be greater than the total # of lieux
                # otherwise there will be duplicated lieux.
                # the issue comes from the fact that the api does not sort the results
                "limit": 25000,
            },
        }
        if location_geo_value is not None:
            default_data["location"]["geoValue"] = location_geo_value

        places_data = []
        page_number = 1
        pbar = None

        while True:
            data = deepcopy(default_data)
            data["options"]["page"] = page_number
            response = self.session.post(
                f"{self.base_url}/new-search",
                json=data,
            )
            response_data = response.json()

            if pbar is None:
                pbar = tqdm(
                    total=response_data["nbResults"],
                    initial=len(response_data["places"]),
                )
            else:
                pbar.update(len(response_data["places"]))

            places_data += response_data["places"]
            page_number += 1

            if len(places_data) >= response_data["nbResults"]:
                break
            elif len(response_data["places"]) == 0:
                break

            # give some slack to the soliguide api
            time.sleep(10)

        if pbar is not None:
            pbar.close()

        return places_data


def extract(url: str, token: str, **kwargs) -> bytes:
    soliguide_client = APIClient(base_url=url, token=token)
    data = soliguide_client.search(
        location_geo_type="pays", location_geo_value="france"
    )
    with io.StringIO() as buf:
        json.dump(data, buf)
        return buf.getvalue().encode()


def html_to_markdown(s: Optional[str]) -> Optional[str]:
    if s is None or s == "":
        return s
    return trafilatura.extract(html.fromstring(s))


def read(path: Path) -> pd.DataFrame:

    # utils.read_json is enough
    # but this adds the conversion of descriptions from html to markdown
    # should eventually be implemented as a python dbt model

    with path.open() as file:
        data = json.load(file)

    for lieu_data in data:
        lieu_data["description"] = html_to_markdown(lieu_data["description"])

        for service_data in lieu_data["services_all"]:
            service_data["description"] = html_to_markdown(service_data["description"])

    df = pd.DataFrame.from_records(data)
    df = df.replace({np.nan: None})

    return df
