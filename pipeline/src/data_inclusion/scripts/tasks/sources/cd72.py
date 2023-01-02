import io
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests
from dateutil.parser import parse as dateutil_parse

from data_inclusion.schema import models
from data_inclusion.scripts.tasks import utils

CD72_SOURCE_STR = "cd72"


def extract_data(src: str, **kwargs) -> dict[str, io.BytesIO]:
    response = requests.get(src)
    return {"data.xlsx": io.BytesIO(response.content)}


def read_data(path: Path) -> tuple[pd.DataFrame, Optional[pd.Series]]:
    df = utils.read_excel(path, sheet_name="Structures")
    return df, df["ID Structure"]


def transform_data(input_df: pd.DataFrame) -> pd.DataFrame:
    raw_structures_df = utils.deserialize_df_data(input_df)

    utils.log_df_info(raw_structures_df)
    structures_df = transform_structure_dataframe(raw_structures_df)
    utils.log_df_info(structures_df)

    return utils.serialize_df_data(structures_df)


def transform_structure_dataframe(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df = input_df.replace("", None)

    # TODO: currently, if 2 rows share the same SIRET, there is no reliable way to
    # identify if one is the parent structure, if both are antennas and the parent
    # structure is missing, or if is an error
    # For now, we drop duplicated rows and keep records containing empty SIRET
    input_df = pd.concat(
        [
            input_df.drop_duplicates(subset="SIRET", keep=False),
            input_df[input_df["SIRET"].isna()],
        ]
    )

    # TODO: confirm treatment of the few records with no ID available
    # Just one outlier at time of writing
    # For now those are dropped
    input_df = input_df[input_df["ID Structure"].notna()]

    output_df = pd.DataFrame()

    # id
    output_df = output_df.assign(id=input_df["ID Structure"])

    # siret
    output_df = output_df.assign(siret=input_df["SIRET"])

    # rna
    output_df = output_df.assign(rna=None)

    # nom
    output_df = output_df.assign(nom=input_df["Nom Structure"])

    # commune
    output_df = output_df.assign(commune=input_df["Ville"])

    # code_postal
    output_df = output_df.assign(code_postal=input_df["Code postal"])

    # code_insee
    output_df = output_df.assign(code_insee=None)

    # adresse
    output_df = output_df.assign(adresse=input_df["Adresse"])

    # complement_adresse
    output_df = output_df.assign(complement_adresse=None)

    # longitude
    output_df = output_df.assign(longitude=None)

    # latitude
    output_df = output_df.assign(latitude=None)

    # typologie
    output_df = output_df.assign(
        typologie=input_df["Typologie structure"].map(
            lambda s: di_typologie_by_input_typologie[s].value
            if s in di_typologie_by_input_typologie
            else models.Typologie.Autre.value
        )
    )

    # telephone
    output_df = output_df.assign(
        telephone=input_df["Téléphone accueil"].fillna(input_df["Téléphone principal"])
    )

    # courriel
    output_df = output_df.assign(courriel=input_df["E-mail accueil"])

    # site_web
    output_df = output_df.assign(site_web=input_df["Site Internet"])

    # presentation_resume
    # presentation_detail
    output_df = output_df.assign(
        presentation_resume=input_df["Description"].map(
            lambda s: (s if len(s) <= 280 else s[:279] + "…") if s is not None else None
        ),
        presentation_detail=input_df["Description"].map(
            lambda s: (None if len(s) <= 280 else s) if s is not None else None
        ),
    )

    # source
    output_df = output_df.assign(source=CD72_SOURCE_STR)

    # date_maj
    output_df = output_df.assign(
        date_maj=input_df["Mis à jour le :"].map(
            lambda s: dateutil_parse(s).isoformat()
        )
    )

    # antenne
    output_df = output_df.assign(antenne=False)

    # lien_source
    output_df = output_df.assign(lien_source=None)

    # horaires_ouverture
    output_df = output_df.assign(horaires_ouverture=input_df["Horaires"])

    # accessibilite
    output_df = output_df.assign(accessibilite=None)

    # labels_nationaux
    di_label_by_input_typologie = {
        (
            "Agence nationale pour la formation professionnelle des adultes (AFPA)"
        ): models.LabelNational.AFPA,
        "Mission Locale": models.LabelNational.MISSION_LOCALE,
    }

    output_df = output_df.assign(
        labels_nationaux=input_df["Typologie structure"].map(
            lambda s: [di_label_by_input_typologie[s].value]
            if s in di_label_by_input_typologie
            else None
        )
    )

    # labels_autres
    output_df = output_df.assign(labels_autres=None)

    # thematiques
    output_df = output_df.assign(thematiques=None)

    output_df = output_df.replace([np.nan, ""], None)

    return output_df


di_typologie_by_input_typologie = {
    "Associations": models.Typologie.ASSO,
    "Autre": models.Typologie.Autre,
    "Organisme de formations": models.Typologie.OF,
    (
        "Structures porteuses d’ateliers et chantiers d’insertion (ACI)"
    ): models.Typologie.ACI,
    "Pôle emploi": models.Typologie.PE,
    "Centre social": models.Typologie.CS,
    "Centres communaux d’action sociale (CCAS)": models.Typologie.CCAS,
    "Communautés de Commune": models.Typologie.CC,
    (
        "Groupements d'employeurs pour l'insertion et la qualification (GEIQ)"
    ): models.Typologie.GEIQ,
    "Municipalités": models.Typologie.MUNI,
    "Entreprise d'insertion (EI)": models.Typologie.EI,
    "Associations intermédiaires (AI)": models.Typologie.AI,
    "Maison de quartier": models.Typologie.MQ,
    "Mission Locale": models.Typologie.ML,
    "Maison des jeunes et de la culture": models.Typologie.MJC,
    "Résidence sociale / FJT - Foyer de Jeunes Travailleurs": models.Typologie.RS_FJT,
    "Entreprise de travail temporaire d'insertion (ETTI)": models.Typologie.ETTI,
    "Points et bureaux information jeunesse (PIJ/BIJ)": models.Typologie.PIJ_BIJ,
    "Chambres consulaires (CCI, CMA, CA)": models.Typologie.Autre,
    (
        "Directions de l’Economie, de l’Emploi, du Travail et des Solidarités (DEETS)"
    ): models.Typologie.DEETS,
    "Plans locaux pour l’insertion et l’emploi (PLIE)": models.Typologie.PLIE,
    "Bibliothèque / Médiathèque": models.Typologie.BIB,
    (
        "Centres d’information sur les droits des femmes et des familles (CIDFF)"
    ): models.Typologie.CIDFF,
    "Conseils Départementaux (CD)": models.Typologie.CD,
    "Caisses d’allocation familiale (CAF)": models.Typologie.CAF,
    (
        "Agence nationale pour la formation professionnelle des adultes (AFPA)"
    ): models.Typologie.AFPA,
    "Préfecture, Sous-Préfecture": models.Typologie.PREF,
    "Région": models.Typologie.REG,
    "Services pénitentiaires d’insertion et de probation (SPIP)": models.Typologie.SPIP,
    "Union Départementale d’Aide aux Familles (UDAF)": models.Typologie.UDAF,
}
