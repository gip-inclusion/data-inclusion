import concurrent.futures
import socket
from dataclasses import astuple, dataclass
from functools import lru_cache

import certifi
import requests
from furl import furl

# 1 second resulted in a LOT of timeouts unfortunately, seems that many
# websites about the topic are quite slow. This results in about ~10s for
# resolving a batch of 1000 URLs
PING_TIMEOUT = 5.0

# Let's use, in nominal conditions, 1 thread per URL. Along with the current
# cache settings, it is the fastest implementation in the typical case.
NUM_WORKERS = 1000

# On average we get ~800 distinct hosts per batch of 1000 but let's
# be safe and use the worst case.
# In some "good" batches though we get the same host a lot of times,
# which improves performance.
NUM_DISTINCT_HOSTS = 1000


# Within the same database session or connection, all pl/Python functions
# share the same socket module.
if not hasattr(socket, "_dns_cache_patched"):
    original_getaddrinfo = socket.getaddrinfo

    @lru_cache(maxsize=NUM_DISTINCT_HOSTS)
    def cached_getaddrinfo(*args, **kwargs):
        return original_getaddrinfo(*args, **kwargs)

    socket.getaddrinfo = cached_getaddrinfo
    socket._dns_cache_patched = True


@dataclass
class CheckedURL:
    input_url: str
    valid_url: str
    status_code: int
    error: str | None


def check_url(input_url, timeout=PING_TIMEOUT, verify=True) -> CheckedURL:
    # most invalid URLs only have a missing schema
    if input_url.startswith("http://") or input_url.startswith("https://"):
        url = input_url
    else:
        url = f"https://{input_url}"
    try:
        verify = certifi.where() if verify else False
        response = requests.get(furl(url).url, timeout=timeout, verify=verify)
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
    except requests.exceptions.SSLError as e:
        if "certificate verify failed: unable to get local issuer certificate" in str(
            e
        ):
            return check_url(input_url, timeout=timeout, verify=False)
        return CheckedURL(
            input_url=input_url,
            valid_url=url,
            status_code=-1,
            error=str(e),
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


def get_host(url: str) -> str:
    """
    Get the host from a URL. If the URL is invalid, return an empty string.
    """
    try:
        return furl(url).host
    except ValueError:
        return None


def check_urls(data: list[str]) -> list[CheckedURL]:
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        results = list(executor.map(check_url, data))
    return [astuple(d) for d in results]
