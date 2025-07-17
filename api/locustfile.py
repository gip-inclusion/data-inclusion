import os
import time

import furl
import locust
from dotenv import load_dotenv

load_dotenv()

LOCUST_API_TOKEN = os.environ.get("LOCUST_API_TOKEN", None)


class APIUser(locust.HttpUser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if LOCUST_API_TOKEN is not None:
            self.client.headers["Authorization"] = f"Bearer {LOCUST_API_TOKEN}"

    def _list_paginated_endpoint(self, url: furl.furl):
        args = "&".join(f"{key}={value}" for key, value in url.args.items())

        for size in [1000]:  # [1000, 5000, 10_000]:
            next_url = url.set(query_params={"size": size})
            start_time = time.time()
            total_length = 0
            while True:
                name = f"{next_url.path}/?size={size}"
                if args:
                    name += f"&{args}"
                data = self.client.get(
                    str(next_url),
                    name=name,
                ).json()
                total_length += len(data["items"])
                match data:
                    case {"items": []}:
                        break
                    case {"next_page": next_page}:
                        next_url.set(query_params={"page": next_page})

            self.environment.events.request.fire(
                request_type="GET",
                name=f"total for {url.path=} {size=}",
                response_time=(time.time() - start_time) * 1000,
                response_length=total_length,
                exception=None,
                context={},
            )

    @locust.task
    @locust.tag("list_structures")
    def list_structures(self):
        self._list_paginated_endpoint(furl.furl("/api/v0/structures"))

    # @locust.task
    # @locust.tag("list_structures_deduplicate")
    # def list_structures_deduplicate(self):
    #     self._list_paginated_endpoint(
    #         furl.furl("/api/v0/structures?exclure_doublons=True")
    #     )

    # listing services takes 3 times longer than structures:
    # - individual calls are slower (to investigate)
    # - pagination is longer (1.5 times more services than structures)
    # Adjusting for this in the task weight enables having about the same total
    # requests for both endpoints in a limited amount of time.
    @locust.task(3)
    @locust.tag("list_services")
    def list_services(self):
        self._list_paginated_endpoint(furl.furl("/api/v0/services"))

    # @locust.task
    # @locust.tag("search_services")
    # def search_services(self):
    #     self._list_paginated_endpoint(
    #         furl.furl(
    #             "/api/v0/search/services",
    #             {
    #                 "code_insee": "59350",
    #                 "thematiques": "sante",
    #             },
    #         )
    #     )

    # @locust.task
    # @locust.tag("search_services_deduplicate")
    # def search_services_deduplicate(self):
    #     self._list_paginated_endpoint(
    #         furl.furl(
    #             "/api/v0/search/services",
    #             {
    #                 "code_insee": "59350",
    #                 "thematiques": "sante",
    #                 "exclure_doublons": "True",
    #             },
    #         )
    #     )
