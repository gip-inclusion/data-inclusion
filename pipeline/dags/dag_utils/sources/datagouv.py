from typing import BinaryIO

from . import utils


class DataGouvClient(utils.BaseApiClient):
    def __init__(
        self,
        base_url: str,
        api_key: str,
    ):
        super().__init__(base_url)
        self.base_url = base_url + "/1"
        self.session.headers.update({"X-API-KEY": api_key})

    def upload_dataset_resource(
        self,
        dataset_id: str,
        resource_id: str,
        buf: BinaryIO,
        filename: str,
    ):
        self.session.post(
            f"{self.base_url}/datasets/{dataset_id}/resources/{resource_id}/upload/",
            files={"file": (filename, buf.getvalue())},
        )
