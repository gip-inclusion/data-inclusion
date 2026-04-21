from data_inclusion.pipeline.common import utils


class MesAidesClient(utils.BaseApiClient):
    # api is quite slow and there is no pagination
    TIMEOUT = 120

    def __init__(self, base_url: str, token: str) -> None:
        super().__init__(base_url)
        self.session.headers.update({"X-Api-Key": token})

    def list_aides(self):
        return self.session.get(
            self.base_url,
            params={"type": "aides"},
            timeout=self.TIMEOUT,
        ).json()["data"]

    def list_garages(self):
        return self.session.get(
            self.base_url,
            params={"type": "garages"},
            timeout=self.TIMEOUT,
        ).json()["data"]
