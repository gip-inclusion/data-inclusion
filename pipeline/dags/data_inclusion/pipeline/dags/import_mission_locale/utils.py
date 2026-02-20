import json

from data_inclusion.pipeline.common import utils


class ImiloClient(utils.BaseApiClient):
    def __init__(self, base_url: str, secret_token: str) -> None:
        super().__init__(base_url)
        self.secret_token = secret_token
        self.session.headers.update({"Authorization": f"Bearer {self.access_token}"})

    # The access token lasts 1h
    @property
    def access_token(self):
        response = self.session.post(
            url=f"{self.base_url}/get_token",
            json={"client_secret": self.secret_token},
        )
        return response.json()["access_token"]

    def list_offres(self) -> list:
        return self.session.get(f"{self.base_url}/get_offres").json()

    def list_structures(self) -> list:
        return self.session.get(f"{self.base_url}/get_structures").json()

    def list_structures_offres(self) -> list:
        return self.session.get(f"{self.base_url}/get_structures_offres").json()


def extract(id: str, url: str, token: str, **kwargs) -> bytes:
    client = ImiloClient(base_url=url, secret_token=token)
    data = getattr(client, f"list_{id}")()
    return json.dumps(data).encode()
