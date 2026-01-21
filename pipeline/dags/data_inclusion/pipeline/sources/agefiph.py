import json

import httpx


def extract(url: str, **kwargs) -> bytes:
    data = []
    next_url = url

    while True:
        response = httpx.get(next_url)
        response.raise_for_status()
        page = response.json()
        data.extend(page["data"])

        if "next" not in page["links"]:
            break
        else:
            next_url = page["links"]["next"]["href"]

    return json.dumps(data).encode()
