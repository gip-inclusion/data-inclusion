import json
import os
from typing import Optional

import requests


class MiloClient:
    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})

    def _make_request(self, endpoint: str, params: Optional[dict] = None):
        try:
            clean_endpoint = endpoint.lstrip("/")
            url = f"{self.base_url}/{clean_endpoint}"

            response = self.session.get(url, params=params)

            if response.status_code == 200:
                return response.json()
            else:
                print(
                    f"""Failed to retrieve data from {url}.
                    Status Code: {response.status_code}"""
                )
                return None
        except Exception as e:
            print(f"Error making request: {str(e)}")
            return None

    def get_token(self) -> Optional[str]:
        try:
            token_url = "https://api-ods.dsiml.org/get_token"
            headers = {"Content-Type": "application/json"}
            client_secret = os.getenv("AIRFLOW_VAR_IMILO_API_TOKEN")

            if not client_secret:
                print("Error: AIRFLOW_VAR_IMILO_API_TOKEN environment variable not set")
                return None

            data = {"client_secret": client_secret}
            response = requests.post(token_url, headers=headers, json=data)

            if response.status_code == 200:
                return response.json().get("access_token")
            else:
                print(f"Failed to retrieve token. Status Code: {response.status_code}")
                print(f"Response Text: {response.text}")
                return None
        except Exception as e:
            print(f"Error getting token: {str(e)}")
            return None

    def _list_paginated_endpoint(
        self, endpoint: str, params: Optional[dict] = None
    ) -> list:
        current_url = endpoint
        return_data = []

        while True:
            response_data = self._make_request(current_url, params)
            if not response_data:
                break

            results = response_data.get("results", [])
            return_data.extend(results)

            next_url = response_data.get("next")
            if not next_url:
                break

            current_url = next_url

        return return_data

    def list_structures(self) -> list:
        return self._list_paginated_endpoint("get_structures")

    def list_services(self) -> list:
        return self._list_paginated_endpoint("offres")

    def list_services_offres(self) -> list:
        return self._list_paginated_endpoint("structures_offres")


def extract(id: str, url: str, token: str, **kwargs) -> bytes:
    try:
        milo_client = MiloClient(base_url=url, token=token)
        method_name = f"list_{id}"

        if not hasattr(milo_client, method_name):
            raise AttributeError(f"Method {method_name} not found in MiloClient")

        data = getattr(milo_client, method_name)()
        return json.dumps(data).encode()
    except Exception as e:
        print(f"Error in extract function: {str(e)}")
        return json.dumps([]).encode()
