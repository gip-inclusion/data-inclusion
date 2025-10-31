import enum

from furl import furl

from data_inclusion.pipeline.dags.import_decoupage_administratif import constants
from data_inclusion.pipeline.sources import utils


class GeoApiClient:
    class Resource(enum.Enum):
        REGIONS = "regions"
        DEPARTEMENTS = "departements"
        EPCIS = "epcis"
        COMMUNES = "communes"
        ARRONDISSEMENTS = "arrondissements"

    def __init__(self) -> None:
        self.base_url = furl(constants.GEO_API_URL)
        # the default zone parameter is inconsistent between resources
        # so we explicitely set it for all resources
        self.base_url.set({"zone": ",".join(["metro", "drom", "com"])})
        self.session = utils.logging_raising_session()

    def list_regions(self) -> list[dict]:
        return self.session.get(str(self.base_url / "regions")).json()

    def list_departements(self) -> list[dict]:
        return self.session.get(str(self.base_url / "departements")).json()

    def list_epcis(self) -> list[dict]:
        return self.session.get(str(self.base_url / "epcis")).json()

    @property
    def communes_url(self) -> furl:
        return (self.base_url / "communes").set(
            {
                # explicitely list retrieve fields
                # to include the "center" field
                "fields": ",".join(
                    [
                        "nom",
                        "code",
                        "centre",
                        "codesPostaux",
                        "codeEpci",
                        "codeDepartement",
                        "codeRegion",
                    ]
                )
            }
        )

    def list_communes(self) -> list[dict]:
        return self.session.get(str(self.communes_url)).json()

    def list_arrondissements(self) -> list[dict]:
        return self.session.get(
            str(self.communes_url.copy().add({"type": "arrondissement-municipal"}))
        ).json()

    def list_resource(self, resource: Resource) -> list[dict]:
        return getattr(self, f"list_{resource.value}")()
