import io
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests

from data_inclusion.schema import models
from data_inclusion.scripts.tasks import utils

CD35_SOURCE_STR = "cd35"


def extract_data(src: str, **kwargs) -> dict[str, io.BytesIO]:
    response = requests.get(src)
    return {"data.csv": io.BytesIO(response.content)}


def read_data(path: Path) -> tuple[pd.DataFrame, Optional[pd.Series]]:
    df = utils.read_csv(path, sep=";")
    return df, df.ORG_ID


def transform_data(input_df: pd.DataFrame) -> pd.DataFrame:
    raw_structures_df = utils.deserialize_df_data(input_df)

    utils.log_df_info(raw_structures_df)
    structures_df = transform_structure_dataframe(raw_structures_df)
    utils.log_df_info(structures_df)

    return utils.serialize_df_data(structures_df)


def transform_structure_dataframe(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df = input_df.replace("", None)

    input_df = input_df.loc[
        ~input_df.ORG_NOM.map(lambda s: "espace" in s.lower() and "jeu" in s.lower())
    ]

    output_df = pd.DataFrame()

    # id
    output_df = output_df.assign(id=input_df.ORG_ID)

    # siret
    output_df = output_df.assign(siret=None)

    # rna
    output_df = output_df.assign(rna=None)

    # nom
    output_df = output_df.assign(nom=input_df.ORG_NOM)

    # commune
    output_df = output_df.assign(commune=input_df.ORG_VILLE)

    # code_postal
    output_df = output_df.assign(code_postal=input_df.ORG_CP)

    # code_insee
    output_df = output_df.assign(code_insee=None)

    # adresse
    output_df = output_df.assign(adresse=input_df.ORG_ADRES)

    # complement_adresse
    output_df = output_df.assign(complement_adresse=None)

    # longitude
    output_df = output_df.assign(
        longitude=pd.to_numeric(input_df.ORG_LONGITUDE, errors="coerce")
    )

    # latitude
    output_df = output_df.assign(
        latitude=pd.to_numeric(input_df.ORG_LATITUDE, errors="coerce")
    )

    # typologie
    DI_STRUCT_TYPE_BY_SIGLE = {
        "CCAS": models.Typologie.CCAS.value,
        "MAIRIE": models.Typologie.MUNI.value,
        "EPHAD": models.Typologie.Autre.value,
        "SAAD": models.Typologie.Autre.value,
    }
    output_df = output_df.assign(
        typologie=input_df.ORG_SIGLE.map(
            lambda s: DI_STRUCT_TYPE_BY_SIGLE.get(s, None) or s
        )
    )

    # telephone
    output_df = output_df.assign(telephone=input_df.ORG_TEL)

    # courriel
    output_df = output_df.assign(courriel=input_df.ORG_MAIL)

    # site_web
    output_df = output_df.assign(site_web=input_df.ORG_WEB)

    # presentation_resume
    # presentation_detail
    output_df = output_df.assign(
        presentation_resume=input_df.ORG_DESC.map(
            lambda s: (s if len(s) <= 280 else s[:279] + "…") if s is not None else None
        ),
        presentation_detail=input_df.ORG_DESC.map(
            lambda s: (None if len(s) <= 280 else s) if s is not None else None
        ),
    )

    # source
    output_df = output_df.assign(source=CD35_SOURCE_STR)

    # date_maj
    output_df = output_df.assign(
        date_maj=input_df.apply(
            lambda row: row.ORG_DATEMAJ or row.ORG_DATECREA, axis=1
        ).map(lambda s: datetime.strptime(s, "%d-%m-%Y").date().isoformat())
    )

    # antenne
    output_df = output_df.assign(antenne=False)

    # lien_source
    output_df = output_df.assign(lien_source=input_df.URL)

    # horaires_ouverture
    output_df = output_df.assign(horaires_ouverture=input_df.ORG_HORAIRE)

    # accessibilite
    output_df = output_df.assign(accessibilite=None)

    # labels_nationaux
    output_df = output_df.assign(labels_nationaux=None)

    # labels_autres
    output_df = output_df.assign(labels_autres=None)

    output_df = output_df.replace([np.nan, ""], None)

    return output_df
