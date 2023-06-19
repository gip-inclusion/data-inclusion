import io
import json
import logging
from typing import Optional

import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)


def log_and_raise(resp: requests.Response, *args, **kwargs):
    try:
        resp.raise_for_status()
    except requests.HTTPError as err:
        logger.error(resp.json())
        raise err


class DoraClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.params.update({"page_size": 1000, "o": "creation_date"})
        self.session.hooks["response"] = [log_and_raise]

    def _list_paginated_endpoint(
        self,
        url_path: str,
        params: Optional[dict] = None,
    ) -> list:
        next_url = f"{self.base_url}{url_path}"
        return_data = []

        pbar = None

        while True:
            response = self.session.get(next_url, params=params)
            data = response.json()

            if pbar is None:
                pbar = tqdm(total=data["count"], initial=len(data["results"]))
            else:
                pbar.update(len(data["results"]))
            return_data += data["results"]
            next_url = data["next"]
            if next_url is None:
                break

        if pbar is not None:
            pbar.close()

        return return_data

    def list_structures(self) -> list:
        return self._list_paginated_endpoint("/structures/")

    def list_services(self) -> list:
        return self._list_paginated_endpoint("/services/")


def extract(id: str, url: str, **kwargs) -> bytes:
    dora_client = DoraClient(base_url=url)
    data = getattr(dora_client, f"list_{id}")()
    with io.StringIO() as buf:
        json.dump(data, buf)
        return buf.getvalue().encode()
