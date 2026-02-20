import httpx


def extract(id: str, url: str, token: str, **kwargs) -> bytes:
    headers = {"Authorization": f"Bearer {token}"}
    response = httpx.post(url, headers=headers)
    response.raise_for_status()
    return response.content
