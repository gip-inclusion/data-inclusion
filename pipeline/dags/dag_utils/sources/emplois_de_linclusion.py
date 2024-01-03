import io
import json
import logging

from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class EmploisClient:
    def __init__(self, base_url: str, token: str) -> None:
        import requests
        from requests.adapters import HTTPAdapter

        self.base_url = base_url.rstrip("/") + "/"
        self.session = requests.Session()
        adapter = HTTPAdapter(
            max_retries=Retry(total=2, backoff_factor=120, status_forcelist=[429])
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({"Authorization": f"Token {token}"})

    def _list_paginated_endpoint(self, url_path: str) -> list:
        from tqdm import tqdm

        next_url = f"{self.base_url}{url_path}"
        structures_data = []

        pbar = None

        while True:
            response = self.session.get(next_url)
            data = response.json()

            if pbar is None:
                pbar = tqdm(total=data["count"], initial=len(data["results"]))
            else:
                pbar.update(len(data["results"]))
            structures_data += data["results"]
            next_url = data["next"]
            if next_url is None:
                break

        if pbar is not None:
            pbar.close()

        return structures_data

    def list_siaes(self) -> list:
        return self._list_paginated_endpoint("?type=siae")

    def list_organisations(self) -> list:
        return self._list_paginated_endpoint("?type=orga")


def extract(id: str, url: str, token: str, **kwargs) -> bytes:
    client = EmploisClient(base_url=url, token=token)

    # raw structures
    data = getattr(client, f"list_{id}")()

    with io.StringIO() as buf:
        json.dump(data, buf)
        return buf.getvalue().encode()
