import io
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pyproj
import requests
from dateutil.parser import parse as dateutil_parse

from data_inclusion.schema import models
from data_inclusion.scripts.tasks import utils

FINESS_SOURCE_STR = "finess"


epsg = {
    # Default value : 2154,
    "GUADELOUPE": 2970,
    "MARTINIQUE": 2970,
    "GUYANE": 2972,
    "LA REUNION": 2975,
    "MAYOTTE": 4471,
    "SAINT PIERRE ET MIQUELON": 4467,
}


def extract_data(src: str, **kwargs) -> dict[str, io.BytesIO]:
    response = requests.get(src)
    return {"data.csv": io.BytesIO(response.content)}


def read_data(path: Path) -> tuple[pd.DataFrame, Optional[pd.Series]]:
    df = utils.read_csv(path, sep=",")
    return df, df.nofinesset


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
    # For now, we drop duplicated rows
    input_df = input_df.drop_duplicates(subset="siret", keep=False)

    # keep social structures only
    input_df = input_df[
        input_df["categagretab"].isin(
            [
                "4301",  # Etab. et Services d'Hébergement pour Adultes Handicapés
                "4302",  # Services de Travail Protégé pour Adultes Handicapés
                "4303",  # Services de Réinsertion Prof pour Adultes Handicapés
                "4501",  # Etablissements de l'Aide Sociale à l'Enfance
                "4601",  # Etablissements pour Adultes et Familles en Difficulté
                "4602",  # Autres Etablissements Sociaux d'Hébergement et d'Accueil
                "4607",  # Logements en Structure Collective
            ]
        )
        # Centre Planification ou Education Familiale Ctre.Planif.Educ.Fam
        | (input_df["categagretab"] == "2202") & (input_df["categetab"] == "228")
        # Centre de soins et de prévention Centre soins prév.
        | (input_df["categagretab"] == "2206") & (input_df["categetab"] == "636")
    ]

    output_df = pd.DataFrame()

    # id
    output_df = output_df.assign(id=input_df.nofinesset)

    # siret
    output_df = output_df.assign(siret=input_df.siret)

    # rna
    output_df = output_df.assign(rna=None)

    # nom
    output_df = output_df.assign(nom=input_df.rs)

    # commune
    output_df = output_df.assign(
        commune=input_df.ligneacheminement.str.extract(r"\d{5} (.*?)(?= CEDEX|$)")
    )

    # code_postal
    output_df = output_df.assign(
        code_postal=input_df.ligneacheminement.map(lambda v: v.split()[0])
    )

    # code_insee
    output_df = output_df.assign(
        # https://www.atih.sante.fr/constitution-codes-geographiques
        code_insee=input_df.departement.str.replace(r"9[A-F]", "97", regex=True)
        + input_df.commune
    )

    output_df = output_df.assign(
        adresse=input_df.apply(
            lambda row: " ".join(
                [
                    v
                    for v in [
                        row.compldistrib,
                        row.numvoie,
                        row.typvoie,
                        row.voie,
                        row.lieuditbp,
                    ]
                    if v
                ]
            ),
            axis=1,
        )
    )

    # complement_adresse
    output_df = output_df.assign(complement_adresse=input_df.compvoie)

    # Transform metropole coordinates
    def coordinates_to_wgs84(df, from_epsg):
        transformer = pyproj.Transformer.from_crs(
            "epsg:" + str(from_epsg), "epsg:4326", always_xy=True
        )
        latitude, longitude = transformer.transform(df["coordxet"], df["coordyet"])
        return latitude, longitude

    wgs84 = coordinates_to_wgs84(input_df, 2154)
    mask = ~input_df.libdepartement.isin(epsg.values())
    input_df.loc[mask, "longitude"] = wgs84[0][mask]
    input_df.loc[mask, "latitude"] = wgs84[1][mask]

    # Transform Territoires d'Outre-Mer coordinates
    for location, code in epsg.items():
        wgs84 = coordinates_to_wgs84(input_df, code)
        mask = input_df.libdepartement == location
        input_df.loc[mask, "longitude"] = wgs84[0][mask]
        input_df.loc[mask, "latitude"] = wgs84[1][mask]

    # longitude
    output_df = output_df.assign(longitude=input_df.longitude)

    # latitude
    output_df = output_df.assign(latitude=input_df.latitude)

    # typologie
    def potential_typologie_from_name(s: str):
        # get a plausible typologie from the structure name
        # there could be several : get the first match
        potential_typologies_dict = {
            models.Typologie.ASE: "ase" in s.split(),
            models.Typologie.ASSO: "association" in s.split() or "asso" in s.split(),
            models.Typologie.CADA: "cada" in s.split(),
            models.Typologie.CAVA: "cava" in s.split(),
            models.Typologie.CCAS: "ccas" in s.split(),
            models.Typologie.CHRS: "chrs" in s.split(),
            models.Typologie.CHU: "chu" in s.split(),
            models.Typologie.CIAS: "cias" in s.split(),
            models.Typologie.CIDFF: "cidff" in s.split(),
            models.Typologie.CSAPA: "csapa" in s.split(),
            models.Typologie.EA: "ea" in s.split(),
            models.Typologie.HUDA: "huda" in s.split(),
            models.Typologie.MDE: "mde" in s.split(),
            models.Typologie.MDEF: "mdef" in s.split(),
            models.Typologie.MJC: "mjc" in s.split(),
            models.Typologie.MSA: "msa" in s.split(),
            models.Typologie.PENSION: "pension" in s.split(),
            models.Typologie.PREVENTION: "prevention" in s.split(),
            models.Typologie.CPH: "cph" in s.split(),
            models.Typologie.UDAF: "udaf" in s.split(),
        }
        return next((k.value for k, v in potential_typologies_dict.items() if v), None)

    output_df = output_df.assign(
        typologie=input_df["rs"]
        .str.lower()
        .map(lambda s: potential_typologie_from_name(s))
    )

    # telephone
    output_df = output_df.assign(telephone=input_df.telephone)

    # courriel
    output_df = output_df.assign(courriel=None)

    # site_web
    output_df = output_df.assign(site_web=None)

    # presentation_resume
    output_df = output_df.assign(presentation_resume=input_df.libcategetab)

    # presentation_detail
    output_df = output_df.assign(presentation_detail=input_df.libcategagretab)

    # source
    output_df = output_df.assign(source=FINESS_SOURCE_STR)

    # date_maj
    output_df = output_df.assign(
        date_maj=input_df.maj.map(lambda s: dateutil_parse(s).isoformat())
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
    output_df = output_df.assign(thematiques=None)

    output_df = output_df.replace([np.nan, ""], None)

    return output_df
