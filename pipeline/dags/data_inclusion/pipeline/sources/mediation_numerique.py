import httpx


def extract(url: str, token: str, **kwargs) -> bytes:
    headers = {"Authorization": f"Bearer {token}"}
    response = httpx.get(url, headers=headers)
    response.raise_for_status()
    return response.content
