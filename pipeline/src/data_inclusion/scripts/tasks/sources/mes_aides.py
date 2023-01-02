import io
import json
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from dateutil.parser import parse as dateutil_parse
from pyairtable import api as pyairtable

from data_inclusion.schema import models
from data_inclusion.scripts.tasks import utils

MES_AIDES_SOURCE_STR = "mes-aides"


def extract_data(src: str, token: str, **kwargs) -> dict[str, io.BytesIO]:
    # extract airtable's base_id and table_name from url
    # cf https://support.airtable.com/docs/understanding-airtable-ids
    base_id = src.split("/")[-3]
    table_name = src.split("/")[-2]

    table = pyairtable.Table(api_key=token, base_id=base_id, table_name=table_name)
    data = table.all()

    with io.StringIO() as buf:
        json.dump(data, buf)
        return {"data.json": io.BytesIO(buf.getvalue().encode())}


def read_data(path: Path) -> tuple[pd.DataFrame, Optional[pd.Series]]:
    df = utils.read_json(path)
    return df, pd.json_normalize(df["fields"])["ID"]


def transform_data(input_df: pd.DataFrame) -> pd.DataFrame:
    raw_garages_df = utils.deserialize_df_data(input_df)

    utils.log_df_info(raw_garages_df)
    structures_df = transform_structure_dataframe(raw_garages_df)
    utils.log_df_info(structures_df)
    services_df = transform_service_dataframe(raw_garages_df)
    utils.log_df_info(services_df)

    return pd.concat(
        [utils.serialize_df_data(df) for df in [structures_df, services_df]]
    )


def transform_structure_dataframe(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df = input_df.replace("", None)

    output_df = pd.DataFrame()

    if len(input_df) == 0:
        return output_df

    input_df = pd.json_normalize(input_df["fields"])
    input_df = input_df.replace({np.nan: None})

    # id
    output_df = output_df.assign(id=input_df["ID"])

    # siret
    # No siret available in data source
    output_df = output_df.assign(siret=None)

    # rna
    output_df = output_df.assign(rna=None)

    # nom
    output_df = output_df.assign(nom=input_df["Nom"])

    # commune
    output_df = output_df.assign(commune=input_df["Ville Nom"])

    # code_postal
    output_df = output_df.assign(code_postal=input_df["Code Postal"])

    # code_insee
    output_df = output_df.assign(code_insee=None)

    # adresse
    output_df = output_df.assign(adresse=input_df["Adresse"])

    # complement_adresse
    output_df = output_df.assign(complement_adresse=None)

    # longitude
    output_df = output_df.assign(longitude=input_df["Ville Longitude"])

    # latitude
    output_df = output_df.assign(latitude=input_df["Ville Latitude"])

    # typologie
    output_df = output_df.assign(typologie=None)

    # telephone
    output_df = output_df.assign(telephone=input_df["Téléphone"])

    # courriel
    output_df = output_df.assign(courriel=input_df["Email"])

    # site_web
    output_df = output_df.assign(site_web=input_df["Url"])

    # presentation_resume
    output_df = output_df.assign(presentation_resume=None)

    # presentation_detail
    output_df = output_df.assign(presentation_detail=None)

    # source
    output_df = output_df.assign(source=MES_AIDES_SOURCE_STR)

    # date_maj
    output_df = output_df.assign(
        date_maj=input_df["Modifié le"].map(lambda s: dateutil_parse(s).isoformat())
    )

    # antenne
    output_df = output_df.assign(antenne=False)

    # lien_source
    output_df = output_df.assign(lien_source=None)

    # horaires_ouverture
    output_df = output_df.assign(horaires_ouverture=None)

    # accessibilite
    output_df = output_df.assign(accessibilite=None)

    # labels_nationaux
    output_df = output_df.assign(labels_nationaux=None)

    # labels_autres
    output_df = output_df.assign(labels_autres=None)

    # thematiques
    output_df = output_df.assign(
        thematiques=[[models.Thematique.MOBILITE.value]] * len(output_df)
    )

    output_df = output_df.replace([np.nan, ""], None)

    return output_df


def transform_service_dataframe(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df = pd.json_normalize(input_df["fields"])
    input_df = input_df.replace({np.nan: None})

    output_df = pd.DataFrame()

    if len(input_df) == 0:
        return output_df

    def get_structure_services(row) -> list:
        return [
            {
                "id": f"{row['ID']}-{service_name.lower().replace(' ', '-')}",
                "structure_id": row["ID"],
                "source": MES_AIDES_SOURCE_STR,
                "nom": service_name,
                "presentation_resume": None,
                "types": [models.TypologieService.AIDE_MATERIELLE.value],
                "thematiques": [models.Thematique.MOBILITE.value],
                "prise_rdv": None,
                "frais": None,
                "frais_autres": None,
                "profils": None,
            }
            for service_name in (row["Services"] or [])
        ]

    output_df = input_df.apply(get_structure_services, axis="columns")
    output_df = pd.json_normalize(output_df)
    output_df = output_df.unstack().dropna().reset_index(drop=True)
    output_df = pd.json_normalize(output_df)

    output_df = output_df.replace([np.nan, ""], None)

    return output_df
