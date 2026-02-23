import io

from data_inclusion.pipeline.common import utils

DATAGOUV_API_URL = "https://www.data.gouv.fr/api/1"
DATAGOUV_API_TOKEN_HEADER = "X-API-KEY"


class DataGouvClient:
    def __init__(self, api_key: str | None) -> None:
        self.base_url = DATAGOUV_API_URL
        self.session = utils.logging_raising_session()

        if api_key is not None:
            self.session.headers.update({DATAGOUV_API_TOKEN_HEADER: api_key})

    def upload_dataset_resource(
        self,
        dataset_id: str,
        resource_id: str,
        buf: io.BytesIO,
        filename: str,
    ) -> None:
        self.session.post(
            f"{self.base_url}/datasets/{dataset_id}/resources/{resource_id}/upload/",
            files={"file": (filename, buf.getvalue())},
        )

    def get_dataset(self, dataset_id: str) -> dict:
        return self.session.get(f"{self.base_url}/datasets/{dataset_id}/").json()
