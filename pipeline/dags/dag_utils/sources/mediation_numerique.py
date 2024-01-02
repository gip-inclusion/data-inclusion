import json
from urllib.parse import urlencode

import requests

from . import utils


def extract(id: str, url: str, **kwargs) -> bytes:
    # TODO(vperron): add a retry mechanism here.
    params = {
        "page[number]": 0,
        "page[size]": 10000,
        "mergedIds[exists]": "false",
    }

    full_data = []
    while True:
        url = f"{utils.safe_urljoin(url, f'{id}-inclusion')}?{urlencode(params)}"
        response = requests.get(url)
        response.raise_for_status()
        raw_data = response.json()
        full_data += raw_data["data"]
        if raw_data["meta"]["number"] == raw_data["meta"]["totalPages"] - 1:
            break
        params["page[number]"] += 1

    return json.dumps(full_data).encode()
