import io
import json
import logging
import time
from copy import deepcopy
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from tqdm import tqdm

from data_inclusion.scripts.tasks import utils

logger = logging.getLogger(__name__)


class APIClient:
    # Documentation on the soliguide API is available here:
    # https://apisolidarite.soliguide.fr/Documentation-technique-de-l-API-Solidarit-ecaf8198f0e9400d93140b8043c9f2ce

    def __init__(self, base_url: str, token: str, user_agent: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"JWT {token}",
                "User-Agent": user_agent,
            }
        )

    def search(
        self,
        location_geo_type: str,
        location_geo_value: Optional[str] = None,
    ) -> dict:
        default_data = {
            "location": {
                "geoType": location_geo_type,
            },
            "options": {
                "limit": 15000,
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


def extract_data(
    src: str, token: str, user_agent: str, **kwargs
) -> dict[str, io.BytesIO]:
    client = APIClient(base_url=src, token=token, user_agent=user_agent)

    # raw places
    data = client.search(
        location_geo_type="pays",
        location_geo_value="france",
    )

    with io.StringIO() as buf:
        json.dump(data, buf)
        return {"data.json": io.BytesIO(buf.getvalue().encode())}


def read_data(path: Path) -> tuple[pd.DataFrame, Optional[pd.Series]]:
    df = utils.read_json(path)
    return df, df.lieu_id