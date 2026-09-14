import httpx
from furl import furl


class BpiClient:
    PAGE_SIZE = 1000

    def __init__(self, base_url: str) -> None:
        self.base_url = furl(
            url=base_url,
            query_params={"size": self.PAGE_SIZE},
        )

    def _list_paginated_endpoint(
        self,
        url_path: str,
    ) -> list:
        next_url = self.base_url / url_path
        next_url.path.normalize()

        items = []

        while True:
            print(f"Fetching {next_url.url}")

            response = httpx.get(next_url.url).raise_for_status()
            data = response.json()

            items += data["items"]
            next_page = data["currentPage"] + 1
            if next_page >= data["totalPages"]:
                break
            next_url.args["page"] = next_page

        return items

    def list_structures(self) -> list:
        return self._list_paginated_endpoint(url_path="/structures")

    def list_services(self) -> list:
        return self._list_paginated_endpoint(url_path="/services")
