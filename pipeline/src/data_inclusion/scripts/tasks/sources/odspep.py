import io
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests

from data_inclusion.schema import models
from data_inclusion.scripts.tasks import utils

ODSPEP_SOURCE_STR = "odspep"


def extract_data(src: str, **kwargs) -> dict[str, io.BytesIO]:
    src = src.rstrip("/")

    return_dict = {}

    for file_name in [
        "ressources.csv",
        "contacts.csv",
        "horaires.csv",
        "familles.csv",
        "categories.csv",
        "sous_categories.csv",
    ]:
        response = requests.get(f"{src}/{file_name}")
        return_dict[file_name] = io.BytesIO(response.content)

    return return_dict


def read_data(path: Path) -> tuple[pd.DataFrame, Optional[pd.Series]]:
    df = utils.read_csv(path, sep="|")
    return df, None


def transform_data(input_df: pd.DataFrame) -> pd.DataFrame:
    df_by_file = {k.split("/")[-1]: v for k, v in input_df.groupby("file")}

    ressources_df = utils.deserialize_df_data(df_by_file["ressources.csv"])
    contacts_df = utils.deserialize_df_data(df_by_file["contacts.csv"])
    horaires_df = utils.deserialize_df_data(df_by_file["horaires.csv"])
    familles_df = utils.deserialize_df_data(df_by_file["familles.csv"])
    categories_df = utils.deserialize_df_data(df_by_file["categories.csv"])
    sous_categories_df = utils.deserialize_df_data(df_by_file["sous_categories.csv"])

    utils.log_df_info(ressources_df)

    structures_df = transform_structure_dataframe(
        ressources_df=ressources_df,
        contacts_df=contacts_df,
        horaires_df=horaires_df,
    )
    utils.log_df_info(structures_df)

    services_df = transform_service_dataframe(
        ressources_df=ressources_df,
        familles_df=familles_df,
        categories_df=categories_df,
        sous_categories_df=sous_categories_df,
    )
    utils.log_df_info(services_df)

    return pd.concat(
        [utils.serialize_df_data(df) for df in [structures_df, services_df]]
    )


def transform_structure_dataframe(
    ressources_df: pd.DataFrame,
    contacts_df: pd.DataFrame,
    horaires_df: pd.DataFrame,
) -> pd.DataFrame:
    ressources_df = ressources_df.replace("", None)
    contacts_df = contacts_df.replace("", None)
    horaires_df = horaires_df.replace("", None)

    ressources_df = ressources_df.merge(contacts_df, how="left", on="ID_RES")
    ressources_df = ressources_df.replace({np.nan: None})

    output_df = pd.DataFrame()

    # id
    # TODO: only services are properly identified in the file. There is no reliable way
    # to prevent duplicated structures rows at this point or to detect antennas.
    output_df = output_df.assign(id=ressources_df["ID_RES"])  # TODO

    # siret
    output_df = output_df.assign(siret=None)

    # rna
    output_df = output_df.assign(rna=None)

    # nom
    output_df = output_df.assign(nom=ressources_df["STRUCTURE"])

    # commune
    output_df = output_df.assign(commune=ressources_df["LIBELLE_COMMUNE_ADR"])

    # code_postal
    output_df = output_df.assign(code_postal=ressources_df["CODE_POSTAL_ADR"])

    # code_insee
    output_df = output_df.assign(code_insee=ressources_df["CODE_COMMUNE_ADR"])

    # adresse
    output_df = output_df.assign(adresse=ressources_df["L4_NUMERO_LIB_VOIE_ADR"])

    # complement_adresse
    output_df = output_df.assign(complement_adresse=ressources_df["L3_COMPLEMENT_ADR"])

    # longitude
    output_df = output_df.assign(longitude=ressources_df["LONGITUDE_ADR"])

    # latitude
    output_df = output_df.assign(latitude=ressources_df["LATITUDE_ADR"])

    # typologie
    def potential_typologie_from_name(s: str):
        # get a plausible typologie from the name
        # there could be several : get the first match
        potential_typologies_dict = {
            models.Typologie.MUNI: "mairie" in s,
            models.Typologie.CAF: "caf" in s.split()
            or ("caisse" in s and "allocation" in s and "fami" in s),
            models.Typologie.CC: "communaut" in s
            and "commune" in s
            and "maternelle" not in s,
            models.Typologie.ASSO: "association" in s.split(),
            models.Typologie.CCAS: "ccas" in s.split()
            or "social" in s
            and "action" in s,
            models.Typologie.CHRS: "chrs" in s.split()
            or ("bergement" in s and "insertion" in s),
            models.Typologie.RS_FJT: ("sidence" in s and "social" in s)
            or "fjt" in s
            or ("foyer" in s and "jeune" in s and "travail" in s),
            models.Typologie.CS: "centre social" in s,
            models.Typologie.MDS: "maison" in s and "solidarit" in s,
            models.Typologie.ML: "mission" in s and "local" in s,
        }
        return next((k.value for k, v in potential_typologies_dict.items() if v), None)

    output_df = output_df.assign(
        typologie=ressources_df["STRUCTURE"]
        .str.lower()
        .map(lambda s: potential_typologie_from_name(s) if s else None)
    )

    # telephone
    output_df = output_df.assign(telephone=ressources_df["TEL_1_CTC"])

    # courriel
    output_df = output_df.assign(courriel=ressources_df["MAIL_CTC"])

    # site_web
    output_df = output_df.assign(site_web=ressources_df["SITE_INTERNET_CTC"])

    # presentation_resume
    output_df = output_df.assign(presentation_resume=None)

    # presentation_detail
    output_df = output_df.assign(presentation_detail=None)

    # source
    output_df = output_df.assign(source=ODSPEP_SOURCE_STR)

    # date_maj
    output_df = output_df.assign(
        date_maj=ressources_df["DATE DERNIERE MAJ"].map(
            lambda s: utils.normalize_datetime(
                datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            ).isoformat()
        )
    )

    # antenne
    output_df = output_df.assign(antenne=False)

    # lien_source
    output_df = output_df.assign(lien_source=None)  # TODO

    # horaires_ouverture
    output_df = output_df.assign(horaires_ouverture=None)  # TODO

    # accessibilite
    output_df = output_df.assign(accessibilite=None)

    # labels_nationaux
    output_df = output_df.assign(labels_nationaux=None)  # TODO

    # labels_autres
    output_df = output_df.assign(labels_autres=None)

    # thematiques
    output_df = output_df.assign(thematiques=None)

    output_df = output_df.replace([np.nan, ""], None)

    return output_df


def transform_service_dataframe(
    ressources_df: pd.DataFrame,
    familles_df: pd.DataFrame,
    categories_df: pd.DataFrame,
    sous_categories_df: pd.DataFrame,
) -> pd.DataFrame:
    ressources_df = ressources_df.replace([np.nan, ""], None)
    familles_df = familles_df.replace("", None)
    categories_df = categories_df.replace("", None)
    sous_categories_df = sous_categories_df.replace("", None)

    output_df = pd.DataFrame()

    if len(ressources_df) == 0:
        return output_df

    # id
    output_df = output_df.assign(id=ressources_df["ID_RES"])

    # structure_id
    output_df = output_df.assign(structure_id=ressources_df["ID_RES"])

    # source
    output_df = output_df.assign(source=ODSPEP_SOURCE_STR)

    # nom
    output_df = output_df.assign(nom=ressources_df["SERVICE_RSP"])

    # presentation_resume
    output_df = output_df.assign(
        presentation_resume=ressources_df["DESCRIPTION_SERVICE"].map(
            lambda s: (s if len(s) <= 280 else s[:279] + "…") if s is not None else None
        )
    )

    # types
    output_df = output_df.assign(types=None)

    # thematiques
    di_thematiques_by_odspep_besoins = {
        "Se déplacer": models.Thematique.MOBILITE,
        "Se loger": None,
        "Se soigner": None,
        "Faire face à des difficultés administratives ou juridiques": None,
        "Faire face à des difficultés financières": None,
        "Sortir de l'isolement et maitriser les savoirs de base": None,
        "Surmonter des contraintes familiales": None,
        "Stratégie de recherche d'emploi": models.Thematique.TROUVER_UN_EMPLOI,
        "Elaboration du projet professionnel": models.Thematique.CHOISIR_UN_METIER,
        "Techniques de recherche d'emploi": models.Thematique.TROUVER_UN_EMPLOI,
        "Retour direct à l'emploi": models.Thematique.TROUVER_UN_EMPLOI,
        "Adaptation au marché du travail": models.Thematique.TROUVER_UN_EMPLOI,
        "Capacités à mobiliser le numérique dans la recherche d'emploi": (
            models.Thematique.NUMERIQUE__FAVORISER_MON_INSERTION_PROFESSIONNELLE
        ),
    }

    output_df = output_df.assign(
        thematiques=ressources_df.ID_RES.map(
            lambda id_: categories_df[categories_df.ID_RES == id_]["Besoin"]
            .map(di_thematiques_by_odspep_besoins)
            .dropna()
            .map(lambda v: v.value)
            .to_list()
        )
    )

    # prise_rdv
    output_df = output_df.assign(prise_rdv=None)

    # frais
    output_df = output_df.assign(frais=None)

    # frais_autres
    output_df = output_df.assign(frais_autres=None)

    # profils
    output_df = output_df.assign(profils=None)

    output_df = output_df.replace([np.nan, ""], None)

    return output_df
