import concurrent.futures

import requests
from pydantic import BaseModel, HttpUrl, ValidationError

# 1 second resulted in a LOT of timeouts unfortunately, seems that many
# websites about the topic are quite slow. This results in about ~10s for
# resolving a batch of 1000 URLs (with 1000 workers, see below)
PING_TIMEOUT = 3.0

# 1000 is a bit arbitrary but since we're mostly IO bound, the assumption is that
# we can have a lot of threads without blocking each other. So we use as many
# threads as we have URLs in a batch.
NUM_WORKERS = 1000


class URLItem(BaseModel):
    url: HttpUrl


def ping_url(input_url, timeout=PING_TIMEOUT):
    # most invalid URLs only have a missing schema
    if input_url.startswith("http://") or input_url.startswith("https://"):
        url = input_url
    else:
        url = f"https://{input_url}"
    try:
        valid_url = URLItem(url=url).url
        response = requests.get(valid_url, timeout=timeout)
        # response.url in case of redirects (allowed by default)
        return input_url, response.url, response.status_code, None
    except ValidationError:
        return input_url, url, 0, "Invalid URL"
    except requests.exceptions.Timeout as e:
        # store the timeout error differently, as we'll want to retry those eventually.
        return input_url, url, -2, str(e)
    except requests.RequestException as e:
        return input_url, url, -1, str(e)


def check_urls(data: list[str]):
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        results = list(executor.map(ping_url, data))
    return results
