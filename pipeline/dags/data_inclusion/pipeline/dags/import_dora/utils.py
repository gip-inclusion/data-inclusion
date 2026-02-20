import json

from data_inclusion.pipeline.common import utils


class DoraClient(utils.BaseApiClient):
    def __init__(self, base_url: str, token: str) -> None:
        super().__init__(base_url)
        self.session.params.update({"page_size": 1000, "o": "creation_date"})
        self.session.headers.update({"Authorization": f"Token {token}"})

    def _list_paginated_endpoint(
        self,
        url_path: str,
        params: dict | None = None,
    ) -> list:
        next_url = f"{self.base_url}{url_path}"
        return_data = []

        while True:
            response = self.session.get(next_url, params=params)
            data = response.json()

            return_data += data["results"]
            next_url = data["next"]
            if next_url is None:
                break

        return return_data

    def list_structures(self) -> list:
        return self._list_paginated_endpoint("/structures/")

    def list_services(self) -> list:
        return self._list_paginated_endpoint("/services/")


def extract(id: str, url: str, token: str, **kwargs) -> bytes:
    dora_client = DoraClient(base_url=url, token=token)
    data = getattr(dora_client, f"list_{id}")()
    return json.dumps(data).encode()
