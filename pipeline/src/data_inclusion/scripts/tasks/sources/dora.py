import io
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

from data_inclusion.schema import models
from data_inclusion.scripts.tasks import utils

DORA_SOURCE_STR = "dora"


logger = logging.getLogger(__name__)


def log_and_raise(resp: requests.Response, *args, **kwargs):
    try:
        resp.raise_for_status()
    except requests.HTTPError as err:
        logger.error(resp.json())
        raise err


class DoraClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.params.update({"page_size": 1000, "o": "creation_date"})
        self.session.hooks["response"] = [log_and_raise]

    def _list_paginated_endpoint(self, url_path: str) -> list:
        next_url = f"{self.base_url}{url_path}"
        return_data = []

        pbar = None

        while True:
            response = self.session.get(next_url)
            data = response.json()

            if pbar is None:
                pbar = tqdm(total=data["count"], initial=len(data["results"]))
            else:
                pbar.update(len(data["results"]))
            return_data += data["results"]
            next_url = data["next"]
            if next_url is None:
                break

        if pbar is not None:
            pbar.close()

        return return_data

    def list_structures(self) -> list:
        return self._list_paginated_endpoint("/structures/")

    def list_services(self) -> list:
        return self._list_paginated_endpoint("/services/")


def extract_data(src: str, **kwargs) -> dict[str, io.BytesIO]:
    dora_client = DoraClient(base_url=src)

    ret = {}

    # raw structures
    data = dora_client.list_structures()

    with io.StringIO() as buf:
        json.dump(data, buf)
        ret["structures.json"] = io.BytesIO(buf.getvalue().encode())

    # raw services
    data = dora_client.list_services()

    with io.StringIO() as buf:
        json.dump(data, buf)
        ret["services.json"] = io.BytesIO(buf.getvalue().encode())

    return ret


def read_data(path: Path) -> tuple[pd.DataFrame, Optional[pd.Series]]:
    df = utils.read_json(path)
    return df, df.id


def transform_data(input_df: pd.DataFrame) -> pd.DataFrame:
    df_by_file = {k.split("/")[-1]: v for k, v in input_df.groupby("file")}
    raw_structures_df = utils.deserialize_df_data(df_by_file["structures.json"])
    raw_services_df = utils.deserialize_df_data(df_by_file["services.json"])

    utils.log_df_info(raw_structures_df)
    structures_df = transform_structure_dataframe(raw_structures_df)
    utils.log_df_info(structures_df)

    utils.log_df_info(raw_services_df)
    services_df = transform_service_dataframe(raw_services_df)
    utils.log_df_info(services_df)

    return pd.concat(
        [utils.serialize_df_data(df) for df in [structures_df, services_df]]
    )


def transform_structure_dataframe(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df = input_df.replace("", None)

    output_df = pd.DataFrame()

    if len(input_df) == 0:
        return output_df

    # id
    output_df = output_df.assign(id=input_df.id)

    # siret
    output_df = output_df.assign(siret=input_df.siret)

    # rna
    output_df = output_df.assign(rna=None)

    # nom
    output_df = output_df.assign(nom=input_df.name)

    # commune
    output_df = output_df.assign(commune=input_df.city)

    # code_postal
    output_df = output_df.assign(code_postal=input_df.postalCode)

    # code_insee
    output_df = output_df.assign(code_insee=input_df.cityCode)

    # adresse
    output_df = output_df.assign(adresse=input_df.address1)

    # complement_adresse
    output_df = output_df.assign(complement_adresse=input_df.address2)

    # longitude
    output_df = output_df.assign(longitude=input_df.longitude)

    # latitude
    output_df = output_df.assign(latitude=input_df.latitude)

    # typologie
    output_df = output_df.assign(
        typologie=input_df.typology.map(lambda v: v["value"] if v is not None else None)
    )

    # telephone
    output_df = output_df.assign(telephone=input_df.phone)

    # courriel
    output_df = output_df.assign(courriel=input_df.email)

    # site_web
    output_df = output_df.assign(site_web=input_df.url)

    # presentation_resume
    output_df = output_df.assign(presentation_resume=input_df.shortDesc)

    # presentation_detail
    output_df = output_df.assign(presentation_detail=input_df.fullDesc)

    # source
    output_df = output_df.assign(source=DORA_SOURCE_STR)

    # date_maj
    output_df = output_df.assign(
        date_maj=input_df.modificationDate.map(
            lambda v: utils.normalize_datetime(datetime.fromisoformat(v)).isoformat()
            if v is not None
            else None
        )
    )

    # antenne
    output_df = output_df.assign(antenne=False)

    # lien_source
    output_df = output_df.assign(lien_source=input_df.linkOnSource)

    # horaires_ouverture
    output_df = output_df.assign(horaires_ouverture=None)

    # accessibilite
    output_df = output_df.assign(accessibilite=None)

    # labels_nationaux
    output_df = output_df.assign(labels_nationaux=None)

    # labels_autres
    output_df = output_df.assign(labels_autres=None)

    # thematiques
    output_df = output_df.assign(thematiques=None)

    output_df = output_df.replace([np.nan, ""], None)

    return output_df


def transform_service_dataframe(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df = input_df.replace("", None)

    output_df = pd.DataFrame()

    if len(input_df) == 0:
        return output_df

    # id
    output_df = output_df.assign(id=input_df.id)

    # structure_id
    output_df = output_df.assign(
        structure_id=input_df.structure.map(lambda v: v.rstrip("/").split("/")[-1])
    )

    # source
    output_df = output_df.assign(source=DORA_SOURCE_STR)

    # nom
    output_df = output_df.assign(nom=input_df.name)

    # presentation_resume
    output_df = output_df.assign(presentation_resume=input_df.shortDesc)

    # types
    output_df = output_df.assign(
        types=input_df.kinds.map(
            lambda l: [
                v["value"]
                for v in l
                if v["value"]
                in [typologie.value for typologie in models.TypologieService]
            ]
        )
    )

    # thematiques
    output_df = output_df.assign(
        thematiques=input_df.categories.map(
            lambda l: [
                v["value"]
                for v in l
                if v["value"] in [thematique.value for thematique in models.Thematique]
            ]
        )
        + input_df.subcategories.map(
            lambda l: [
                v["value"]
                for v in l
                if v["value"] in [thematique.value for thematique in models.Thematique]
            ]
        )
    )

    # prise_rdv
    output_df = output_df.assign(prise_rdv=input_df.onlineForm)

    # frais
    output_df = output_df.assign(frais=None)

    # frais_autres
    output_df = output_df.assign(frais_autres=input_df.feeDetails)

    # profils
    output_df = output_df.assign(profils=None)

    output_df = output_df.replace([np.nan, ""], None)

    return output_df
