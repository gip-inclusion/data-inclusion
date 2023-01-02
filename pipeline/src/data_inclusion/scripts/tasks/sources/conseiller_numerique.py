import io
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests

from data_inclusion.scripts.tasks import utils


def extract_data(src: str, **kwargs) -> dict[str, io.BytesIO]:
    src = src.rstrip("/")

    return_dict = {}

    for file_name in ["structures.json", "services.json"]:
        response = requests.get(f"{src}/{file_name}")
        return_dict[file_name] = io.BytesIO(response.content)

    return return_dict


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
