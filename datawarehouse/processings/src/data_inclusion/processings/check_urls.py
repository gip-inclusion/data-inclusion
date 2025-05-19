import concurrent.futures
from dataclasses import astuple, dataclass

import requests
from furl import furl

# 1 second resulted in a LOT of timeouts unfortunately, seems that many
# websites about the topic are quite slow. This results in about ~10s for
# resolving a batch of 1000 URLs (with 10_000 workers, see below)
PING_TIMEOUT = 5.0

# 1000 is a bit arbitrary but since we're mostly IO bound, the assumption is that
# we can have a lot of threads without blocking each other. So we use as many
# threads as we have URLs in a batch.
NUM_WORKERS = 10_000


@dataclass
class CheckedURL:
    input_url: str
    valid_url: str
    status_code: int
    error: str | None


def check_url(input_url, timeout=PING_TIMEOUT) -> CheckedURL:
    # most invalid URLs only have a missing schema
    if input_url.startswith("http://") or input_url.startswith("https://"):
        url = input_url
    else:
        url = f"https://{input_url}"
    try:
        response = requests.get(furl(url).url, timeout=timeout)
        return CheckedURL(
            input_url=input_url,
            valid_url=response.url,  # in case of redirects (allowed by default)
            status_code=response.status_code,
            error=None,
        )
    except ValueError:
        return CheckedURL(
            input_url=input_url,
            valid_url=url,
            status_code=0,
            error="Invalid URL",
        )
    except requests.exceptions.Timeout as e:
        return CheckedURL(
            input_url=input_url,
            valid_url=url,
            status_code=-2,
            error=str(e),
        )
    except requests.RequestException as e:
        return CheckedURL(
            input_url=input_url,
            valid_url=url,
            status_code=-1,
            error=str(e),
        )


def check_urls(data: list[str]) -> list[CheckedURL]:
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        results = list(executor.map(check_url, data))
    for x in results:
        print(x.input_url, x.status_code, x.error)
    return [astuple(d) for d in results]
