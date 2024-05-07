import json
import logging

from . import utils

logger = logging.getLogger(__name__)

FIELDS = [
    "ID",
    "NOM",
    "SIGLE",
    "ADRESSE",
    "COMPLEMENT_ADRESSE",
    "CODE_POSTAL",
    "COMMUNE",
    "CODE_INSEE",
    "LONGITUDE",
    "LATITUDE",
    "TELEPHONE",
    "COURRIEL",
    "SITE_WEB",
    "HORAIRES_OUVERTURE",
    "PRESENTATION_DETAIL",
    "DATE_CREATION",
    "DATE_MAJ",
    "PROFIL",
    "THEMATIQUE",
    "LIEN_SOURCE",
]

QUERY_PARAMS = {
    "where": "1=1",  # where clause is mandatory.
    "f": "json",
    "outFields": ",".join(FIELDS),
    "outSR": 4326,
}

ARCGIS_MAX_PAGE_SIZE = 1000


class ArcgisCD35Client(utils.BaseApiClient):
    def list_organismes(self) -> list:
        from furl import furl

        organismes = []
        offset = 0

        while True:
            params = QUERY_PARAMS.copy()
            params["resultOffset"] = offset
            next_url = furl(self.base_url).add(params)
            logger.info("Retrieving from URL=%s", next_url)
            response = self.session.get(next_url)
            new_features = response.json()["features"]
            if not new_features:
                break
            offset += len(new_features)
            organismes += [feat["attributes"] for feat in new_features]

        return organismes


def extract(id: str, url: str, **kwargs) -> bytes:
    client = ArcgisCD35Client(base_url=url)
    return json.dumps(client.list_organismes()).encode()
