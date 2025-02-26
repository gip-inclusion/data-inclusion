import json
import time
from copy import deepcopy
from math import ceil
from pathlib import Path
from typing import Optional

from . import utils


class APIClient(utils.BaseApiClient):
    # Documentation on the soliguide API is available here:
    # https://apisolidarite.soliguide.fr/solinum/Technical-Documentation-of-the-Solidarity-API-59cb0fe101274d74b9a7c3729cf473b2

    def __init__(self, base_url: str, token: str):
        super().__init__(base_url)
        self.session.headers.update({"Authorization": f"JWT {token}"})

    def search(
        self,
        location_geo_type: str,
        location_geo_value: Optional[str] = None,
    ) -> list[dict]:
        if location_geo_type != "position" and location_geo_value is None:
            raise RuntimeError("Missing location.geoValue.")

        default_data = {
            "location": {
                "geoType": location_geo_type,
            },
            "options": {
                # this value must be greater than the total # of lieux
                # otherwise there will be duplicated lieux.
                # the issue comes from the fact that the api does not sort the results
                "limit": 25000,
            },
        }
        if location_geo_value is not None:
            default_data["location"]["geoValue"] = location_geo_value

        places_data = []
        page_number = 1
        total_number_of_pages = None

        while True:
            data = deepcopy(default_data)
            data["options"]["page"] = page_number
            response = self.session.post(
                utils.safe_urljoin(self.base_url, "new-search"),
                json=data,
            )
            response_data = response.json()

            places_data += response_data["places"]
            page_number += 1

            if total_number_of_pages is None:
                total_number_of_pages = ceil(
                    response_data["nbResults"] / data["options"]["limit"]
                )

            if page_number > total_number_of_pages:
                break
            elif len(response_data["places"]) == 0:
                break

            # give some slack to the soliguide api
            time.sleep(10)

        return places_data


def extract(url: str, token: str, **kwargs) -> bytes:
    soliguide_client = APIClient(base_url=url, token=token)
    data = soliguide_client.search(location_geo_type="pays", location_geo_value="fr")
    return json.dumps(data).encode()


def read(path: Path):
    import pandas as pd

    from . import utils

    # utils.df_from_json is enough
    # but this adds the conversion of descriptions from html to markdown
    # should eventually be implemented as a python dbt model

    with path.open() as file:
        data = json.load(file)

    for lieu_data in data:
        lieu_data["description"] = utils.html_to_markdown(lieu_data["description"])

        for service_data in lieu_data["services_all"]:
            service_data["description"] = utils.html_to_markdown(
                service_data["description"]
            )

    df = pd.DataFrame.from_records(data)
    return utils.df_clear_nan(df)
