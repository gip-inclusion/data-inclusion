import json

from urllib3.util.retry import Retry

from . import utils


class EmploisClient(utils.BaseApiClient):
    def __init__(self, base_url: str, token: str) -> None:
        from requests.adapters import HTTPAdapter

        super().__init__(base_url)
        adapter = HTTPAdapter(
            max_retries=Retry(total=2, backoff_factor=120, status_forcelist=[429])
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({"Authorization": f"Token {token}"})

    def list_organisations(self) -> list:
        next_url = f"{self.base_url}?type=orga&page_size=1000"
        structures_data = []

        while True:
            response = self.session.get(next_url)
            data = response.json()

            structures_data += data["results"]
            next_url = data["next"]
            if next_url is None:
                break

        return structures_data


def extract_organisations(url: str, token: str, **kwargs) -> bytes:
    client = EmploisClient(base_url=url, token=token)
    return json.dumps(client.list_organisations()).encode()
