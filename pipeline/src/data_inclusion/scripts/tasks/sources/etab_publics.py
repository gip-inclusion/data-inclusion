import io
import json
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests

from data_inclusion.schema import models
from data_inclusion.scripts.tasks import utils

ETAB_PUBLICS_SOURCE_STR = "etab_publics"

pivots_of_interest = [
    "agefiph",
    "agence_insertion",
    "adil",
    "ars",
    "aav",
    "afpa",
    "apec",
    "aract",
    "apecita",
    "bav",
    "caf",
    "cicas",
    "cio",
    "cidf",
    "pmi",
    "dr_femmes",
    "ars_antenne",
    "direccte",
    "direccte_ut",
    "drihl",
    "drihl_ut",
    "dd_femmes",
    "msap",
    "greta",
    "cij",
    "epci",
    "mairie",
    "mairie_com",
    "maison_emploi",
    "mjd",
    "maison_handicapees",
    "mission_locale",
    "msa",
    "permanence_juridique",
    "plateforme_naturalisation",
    "pcb",
    "pif",
    "prefecture",
    "droit_travail",
]


def extract_data(src: str, **kwargs) -> dict[str, io.BytesIO]:
    response = requests.get(src)
    return {"data.tar.bz2": io.BytesIO(response.content)}


def read_data(path: Path) -> tuple[pd.DataFrame, Optional[pd.Series]]:
    with tarfile.open(path, "r:bz2") as tar:
        tar.extractall(path=path.parent)

    with next(path.parent.glob("*.gouv_local.json")).open() as f:
        data = json.load(f)

    df = pd.json_normalize(data["service"], max_level=0)
    return df, df.id


def transform_data(input_df: pd.DataFrame) -> pd.DataFrame:
    raw_structures_df = utils.deserialize_df_data(input_df)

    utils.log_df_info(raw_structures_df)
    structures_df = transform_structure_dataframe(raw_structures_df)
    utils.log_df_info(structures_df)

    return utils.serialize_df_data(structures_df)


def transform_structure_dataframe(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df = input_df.replace("", None)

    output_df = pd.DataFrame()

    # filter out uninteresting data
    types_df = pd.json_normalize(
        input_df.to_dict(orient="records"),
        record_path="pivot",
        meta="id",
    )
    filtered_types_df = types_df[types_df.type_service_local.isin(pivots_of_interest)]
    input_df = pd.merge(
        input_df, filtered_types_df[["id", "type_service_local"]], how="left", on="id"
    )

    # id
    output_df = output_df.assign(id=input_df.id)

    # siret
    output_df = output_df.assign(siret=input_df.siret.fillna(input_df.siren))

    # rna
    output_df = output_df.assign(rna=None)

    # nom
    output_df = output_df.assign(nom=input_df.nom)

    adresses_df = pd.json_normalize(
        input_df.to_dict(orient="records"),
        record_path="adresse",
        meta="id",
    )
    adresses_df = adresses_df.sort_values(by="type_adresse")
    adresses_df = adresses_df.drop_duplicates(subset=["id"])
    input_df = pd.merge(input_df, adresses_df, how="left", on="id")

    # commune
    output_df = output_df.assign(commune=input_df.nom_commune)

    # code_postal
    output_df = output_df.assign(code_postal=input_df.code_postal)

    # code_insee
    output_df = output_df.assign(code_insee=input_df.code_insee_commune)

    # adresse
    output_df = output_df.assign(adresse=input_df.numero_voie)

    # complement_adresse
    output_df = output_df.assign(complement_adresse=input_df.complement1)

    # longitude
    output_df = output_df.assign(longitude=input_df.longitude.astype(str))

    # latitude
    output_df = output_df.assign(latitude=input_df.latitude.astype(str))

    # typologie
    def potential_typologie_from_pivot(s: str):
        # get a plausible typologie from the structure pivot code
        # there could be several : get the first match
        potential_typologies_dict = {
            models.Typologie.AFPA: s == "afpa",
            models.Typologie.CAF: s == "caf",
            models.Typologie.CIDFF: s in ["dr_femmes", "dd_femmes"],
            models.Typologie.MUNI: s in ["mairie", "mairie_com"],
            models.Typologie.MDE: s == "maison_emploi",
            models.Typologie.MDPH: s == "maison_handicapees",
            models.Typologie.ML: s == "mission_locale",
            models.Typologie.MSA: s == "msa",
            models.Typologie.PREF: s == "prefecture",
            models.Typologie.MDPH: s == "maison_handicapees",
        }
        return next((k.value for k, v in potential_typologies_dict.items() if v), None)

    output_df = output_df.assign(
        typologie=input_df.type_service_local.map(
            lambda s: potential_typologie_from_pivot(s)
        )
    )

    # telephone
    telephone_df = pd.json_normalize(
        input_df.to_dict(orient="records"),
        record_path="telephone",
        meta="id",
    )
    telephone_df = telephone_df.drop_duplicates(subset=["id"])
    telephone_df = telephone_df[["id", "valeur"]]
    telephone_df = telephone_df.rename(columns={"valeur": "telephone_valeur"})
    input_df = pd.merge(input_df, telephone_df, how="left", on="id")

    output_df = output_df.assign(telephone=input_df.telephone_valeur)

    # courriel
    courriel_df = pd.json_normalize(
        input_df.to_dict(orient="records"),
        record_path="adresse_courriel",
        meta="id",
    )
    courriel_df = courriel_df.drop_duplicates(subset=["id"])
    courriel_df = courriel_df.rename(columns={0: "courriel_valeur"})
    courriel_df = courriel_df[["id", "courriel_valeur"]]
    input_df = pd.merge(input_df, courriel_df, how="left", on="id")

    output_df = output_df.assign(courriel=input_df.courriel_valeur)

    # site_web
    site_internet_df = pd.json_normalize(
        input_df.to_dict(orient="records"),
        record_path="site_internet",
        meta="id",
    )
    site_internet_df = site_internet_df.drop_duplicates(subset=["id"])
    site_internet_df = site_internet_df[["id", "valeur"]]
    site_internet_df = site_internet_df.rename(
        columns={"valeur": "site_internet_valeur"}
    )
    input_df = pd.merge(input_df, site_internet_df, how="left", on="id")

    output_df = output_df.assign(site_web=input_df.site_internet_valeur)

    # presentation_resume
    # presentation_detail
    output_df = output_df.assign(
        presentation_resume=input_df.mission.map(
            lambda s: (s if len(s) <= 280 else s[:279] + "…") if s is not None else None
        ),
        presentation_detail=input_df.mission.map(
            lambda s: (None if len(s) <= 280 else s) if s is not None else None
        ),
    )

    # source
    output_df = output_df.assign(source=ETAB_PUBLICS_SOURCE_STR)

    # date_maj
    output_df = output_df.assign(
        date_maj=input_df.date_modification.map(
            lambda v: utils.normalize_datetime(
                datetime.strptime(v, "%d/%m/%Y %H:%M:%S")
            ).isoformat()
        )
    )

    # antenne
    output_df = output_df.assign(antenne=False)

    # lien_source
    output_df = output_df.assign(lien_source=None)

    # horaires_ouverture
    output_df = output_df.assign(
        horaires_ouverture=input_df.plage_ouverture.map(json.dumps)
    )

    # accessibilite
    output_df = output_df.assign(accessibilite=None)

    # labels_nationaux
    def labels_nationaux_from_row(row):
        # get a plausible typologie from the structure pivot code
        # there could be several : get the first match
        potential_labels_dict = {
            models.LabelNational.CAF: row.pivot == "caf",
            models.LabelNational.FRANCE_SERVICE: row.partenaire == "france_service",
            models.LabelNational.AFPA: row.pivot == "afpa",
            models.LabelNational.MISSION_LOCALE: row.pivot == "mission_locale",
            models.LabelNational.MDPH: row.pivot == "maison_handicapees",
        }
        return [k.value for k, v in potential_labels_dict.items() if v]

    output_df = output_df.assign(
        labels_nationaux=input_df.apply(
            lambda row: labels_nationaux_from_row(row), axis="columns"
        )
    )

    # labels_autres
    output_df = output_df.assign(labels_autres=None)

    # thematiques
    output_df = output_df.assign(thematiques=None)

    output_df = output_df.replace([np.nan, ""], None)

    return output_df
