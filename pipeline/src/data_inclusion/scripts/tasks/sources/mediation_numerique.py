import io
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests

from data_inclusion.scripts.tasks import utils


def get_resources_url_from_dataset_url(dataset_url: str) -> dict[str, str]:
    """Identify data.inclusion resource urls in a data.gouv dataset given its url"""

    dataset_id = dataset_url.rstrip("/").split("/")[-1]

    response = requests.get(f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}")
    dataset_data = response.json()

    # filter resources based on the data.inclusion schema
    data_inclusion_resources = [
        resource_data
        for resource_data in dataset_data["resources"]
        if resource_data["schema"]["name"] == "betagouv/data-inclusion-schema"
    ]

    # identify urls based on resource titles
    return {
        "structures"
        if "structures" in resource_data["title"]
        else "services": resource_data["latest"]
        for resource_data in data_inclusion_resources
    }


def extract_data(src: str, **kwargs) -> dict[str, io.BytesIO]:
    urls = get_resources_url_from_dataset_url(src)

    ret = {}

    # structures
    response = requests.get(urls["structures"])
    ret["structures.json"] = io.BytesIO(response.content)

    # services
    response = requests.get(urls["services"])
    ret["services.json"] = io.BytesIO(response.content)

    return ret


def read_data(path: Path) -> tuple[pd.DataFrame, Optional[pd.Series]]:
    df = utils.read_json(path)
    return df, df.id


def transform_data(input_df: pd.DataFrame) -> pd.DataFrame:
    df_by_file = {k.split("/")[-1]: v for k, v in input_df.groupby("file")}
    structures_df = utils.deserialize_df_data(df_by_file["structures.json"])
    services_df = utils.deserialize_df_data(df_by_file["services.json"])

    utils.log_df_info(structures_df)
    structures_df = transform_structure_dataframe(structures_df)
    utils.log_df_info(structures_df)

    utils.log_df_info(services_df)
    services_df = transform_service_dataframe(services_df)
    utils.log_df_info(services_df)

    return pd.concat(
        [utils.serialize_df_data(df) for df in [structures_df, services_df]]
    )


def transform_structure_dataframe(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df = input_df.replace(["", np.nan], None)

    if len(input_df) == 0:
        return input_df

    # siret
    input_df = input_df.assign(siret=input_df.siret.replace(14 * "0", None))

    # antenne
    input_df = input_df.assign(antenne=False)

    return input_df


def transform_service_dataframe(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df = input_df.replace(["", np.nan], None)

    return input_df
