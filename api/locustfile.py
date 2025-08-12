import os

import furl
import locust
from dotenv import load_dotenv

load_dotenv()

LOCUST_API_TOKEN = os.environ.get("LOCUST_API_TOKEN", None)
LOCUST_API_VERSION = os.environ.get("LOCUST_API_VERSION", "v0")


class APIUser(locust.HttpUser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if LOCUST_API_TOKEN is not None:
            self.client.headers["Authorization"] = f"Bearer {LOCUST_API_TOKEN}"

    def _list_paginated_endpoint(self, url: furl.furl):
        args = "&".join(f"{key}={value}" for key, value in url.args.items())
        next_url = url.add(query_params={"page": 1})

        while True:
            name = f"{next_url.path}/?page=[{(next_url.args['page'] // 10) * 10:3d}]"
            if args:
                name += f"&{args}"
            match self.client.get(
                str(next_url),
                name=name,
            ).json():
                case {"items": []}:
                    break
                case {"page": current_page}:
                    next_url.args["page"] = current_page + 1

    @locust.task
    @locust.tag("list_structures")
    def list_structures(self):
        self._list_paginated_endpoint(
            furl.furl(f"/api/{LOCUST_API_VERSION}/structures")
        )

    @locust.task
    @locust.tag("list_structures_deduplicate")
    def list_structures_deduplicate(self):
        self._list_paginated_endpoint(
            furl.furl(f"/api/{LOCUST_API_VERSION}/structures?exclure_doublons=True")
        )

    @locust.task
    @locust.tag("list_services")
    def list_services(self):
        self._list_paginated_endpoint(furl.furl(f"/api/{LOCUST_API_VERSION}/services"))

    @locust.task
    @locust.tag("search_services")
    def search_services(self):
        self._list_paginated_endpoint(
            furl.furl(
                f"/api/{LOCUST_API_VERSION}/search/services",
                {
                    "code_insee": "59350",
                    "thematiques": "sante",
                },
            )
        )

    @locust.task
    @locust.tag("search_services_deduplicate")
    def search_services_deduplicate(self):
        self._list_paginated_endpoint(
            furl.furl(
                f"/api/{LOCUST_API_VERSION}/search/services",
                {
                    "code_insee": "59350",
                    "thematiques": "sante",
                    "exclure_doublons": "True",
                },
            )
        )
