import json
from enum import Enum

from . import utils


class StructureType(Enum):
    CCAS = 15
    MAISON_DEPARTEMENTALE_DE_L_AUTONOMIE = 53
    MAISON_DEPARTEMENTALE_DES_SOLIDARITES = 127


class Client:
    def __init__(self, base_url: str, key: str) -> None:
        self.base_url = base_url.rstrip("/") + "/v1/client"
        self.session = utils.logging_raising_session()
        self.session.headers.update({"x-api-key": key})

    def list_structures(self, id_type_structure: int) -> list:
        url = self.base_url + "/structures"
        params = {
            "idTypeStructure": id_type_structure,
            "page": 1,
        }
        results = []

        while (
            (response := self.session.get(url=str(url), params=params))
            and (structures := response.json()["structures"])
            and len(structures) > 0
        ):
            results.extend(structures)
            params["page"] += 1

        return results


def extract(id: str, url: str, token: str, **kwargs) -> bytes:
    client = Client(base_url=url, key=token)
    data = [
        structure
        for structure_type in StructureType
        for structure in client.list_structures(structure_type.value)
    ]
    return json.dumps(data).encode()
