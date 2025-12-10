import enum

from furl import furl

from data_inclusion.pipeline.common import utils
from data_inclusion.pipeline.dags.import_decoupage_administratif import constants

COMMUNES_FIELDS = [
    "nom",
    "code",
    "centre",  # must be explicitly included
    "codesPostaux",
    "codeEpci",
    "codeDepartement",
    "codeRegion",
    "chefLieu",  # for communes associées déléguées
]


def set_communes_query_params(base_url: furl) -> furl:
    return base_url.copy().set({"fields": ",".join(COMMUNES_FIELDS)})


class GeoApiClient:
    class Resource(enum.Enum):
        REGIONS = "regions"
        DEPARTEMENTS = "departements"
        EPCIS = "epcis"
        COMMUNES = "communes"
        ARRONDISSEMENTS = "arrondissements"
        COMMUNES_ASSOCIEES_DELEGUEES = "communes_associees_deleguees"

    def __init__(self) -> None:
        self.base_url = furl(constants.GEO_API_URL)
        # the default zone parameter is inconsistent between resources
        # so we explicitely set it for all resources
        self.base_url.set({"zone": ",".join(["metro", "drom", "com"])})
        self.session = utils.logging_raising_session()

    def list_regions(self) -> list[dict]:
        return self.session.get(str(self.base_url / self.Resource.REGIONS.value)).json()

    def list_departements(self) -> list[dict]:
        return self.session.get(
            str(self.base_url / self.Resource.DEPARTEMENTS.value)
        ).json()

    def list_epcis(self) -> list[dict]:
        return self.session.get(str(self.base_url / self.Resource.EPCIS.value)).json()

    def list_communes(self) -> list[dict]:
        return self.session.get(
            str(set_communes_query_params(self.base_url / self.Resource.COMMUNES.value))
        ).json()

    def list_arrondissements(self) -> list[dict]:
        return self.session.get(
            str(
                set_communes_query_params(
                    self.base_url / self.Resource.COMMUNES.value
                ).add({"type": "arrondissement-municipal"})
            )
        ).json()

    def list_communes_associees_deleguees(self) -> list[dict]:
        return self.session.get(
            str(
                set_communes_query_params(
                    self.base_url / self.Resource.COMMUNES_ASSOCIEES_DELEGUEES.value
                )
            )
        ).json()

    def list_resource(self, resource: Resource) -> list[dict]:
        return getattr(self, f"list_{resource.value}")()
