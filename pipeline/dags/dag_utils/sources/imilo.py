import json

import requests

from airflow.models import Variable


class ImiloClient:
    def __init__(self, base_url: str, secret: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self.secret = secret

    def _get_token(self):
        response = self.session.post(
            url=f"{self.base_url}/get_token",
            data=json.dumps(
                {
                    "client_secret": self.secret,
                }
            ),
        )
        response.raise_for_status()
        self.session.headers.update(
            {"Authorization": f"Bearer {response.json()["access_token"]}"}
        )

    def _get_endpoint(
        self,
        url_path: str,
    ) -> list:
        next_url = f"{self.base_url}{url_path}"
        response = self.session.get(next_url)
        if response.status_code == 401:
            self._get_token()
            response = self.session.get(next_url)
        response.raise_for_status()
        return response.json()

    def list_offres(self) -> list:
        return self._get_endpoint("/get_offres")

    def list_structures(self) -> list:
        return self._get_endpoint("/get_structures")

    def list_structures_offres(self) -> list:
        return self._get_endpoint("/get_structures_offres")


client = ImiloClient(
    base_url=Variable.get("IMILO_API_URL"),
    secret=Variable.get("IMILO_API_SECRET"),
)


def extract(id: str, url: str, token: str, **kwargs) -> bytes:
    data = getattr(client, f"list_{id}")()
    return json.dumps(data).encode()
