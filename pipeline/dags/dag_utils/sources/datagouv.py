import logging
from typing import BinaryIO

import requests

logger = logging.getLogger(__name__)


def log_and_raise(resp: requests.Response, *args, **kwargs):
    try:
        resp.raise_for_status()
    except requests.HTTPError as err:
        logger.error(resp.json())
        raise err


class DataGouvClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
    ):
        self.base_url = base_url + "/1"
        self.session = requests.Session()
        self.session.hooks["response"] = [log_and_raise]
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
