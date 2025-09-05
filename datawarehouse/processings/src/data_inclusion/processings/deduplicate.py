import json
import logging
import os
from dataclasses import dataclass

import dedupe
import pandas as pd
import phonenumbers
from unidecode import unidecode

THRESHOLD = 0.75  # as defined in our Jupyter simulations


logger = logging.getLogger(__name__)


@dataclass
class DeduplicateInput:
    _di_surrogate_id: str
    adresse: str
    code_insee: str
    code_postal: str
    commune: str
    courriel: str
    date_maj: str
    latitude: float
    longitude: float
    nom: str
    siret: str
    source: str
    telephone: str


def _format_phonenumber(s):
    if not s:
        return None
    try:
        p = phonenumbers.parse(s, "FR")
    except phonenumbers.phonenumberutil.NumberParseException:
        return None
    return phonenumbers.format_number(p, phonenumbers.PhoneNumberFormat.NATIONAL)


def deduplicate(data: list[DeduplicateInput]) -> pd.DataFrame:
    df = pd.DataFrame.from_records(data)

    # focus on "quality" sources
    df = df.loc[
        df["source"].isin(
            [
                "action-logement",
                "dora",
                "cd35",
                "fredo",
                "emplois-de-linclusion",
                "france-travail",
                "mediation-numerique",
                "mes-aides",
                "soliguide",
            ]
        )
    ]

    # exclude structures with long surrogate_id (mednum...)
    df = df.loc[df["_di_surrogate_id"].str.len() <= 256]

    # ignore structures with no city code (no address, no geolocation)
    df = df.loc[df["code_insee"].notnull()]

    # some cleanups and formatting
    df["id"] = df["_di_surrogate_id"]
    df["date_maj"] = pd.to_datetime(df["date_maj"], errors="coerce").dt.strftime(
        "%m/%d/%Y"
    )
    df["nom"] = df["nom"].str.lower().str.strip().apply(unidecode)
    df["location"] = df.apply(
        lambda row: [
            float(row["latitude"] if pd.notnull(row["latitude"]) else 0.0),
            float(row["longitude"] if pd.notnull(row["longitude"]) else 0.0),
        ],
        axis=1,
    )
    df["telephone"] = df["telephone"].apply(_format_phonenumber)

    # including the SIREN helps the algorithm converge
    df["siren"] = df["siret"].str[:9]

    df = df[
        [
            "id",
            "source",
            "date_maj",
            "nom",
            "commune",
            "adresse",
            "location",
            "code_postal",
            "code_insee",
            "siret",
            "siren",
            "telephone",
            "courriel",
        ]
    ]

    # dedupe does not handle empty values, only None
    df = df.replace("", None)

    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(f"{dir_path}/deduplicate-model.bin", "rb") as f:
        deduper = dedupe.StaticDedupe(f)

    data = json.loads(df.to_json(orient="records"))

    logger.info(f"partitioning data into clusters with {THRESHOLD=}")
    clustered_dupes = deduper.partition(
        data={d["id"]: d for d in data},
        threshold=THRESHOLD,
    )

    logger.info("exporting clusters to JSON")
    clusters_df = pd.DataFrame(
        data=[
            {"structure_id": record_id, "cluster_id": cluster_id, "score": score}
            for cluster_id, (records, scores) in enumerate(clustered_dupes)
            for record_id, score in zip(records, scores)
        ]
    )
    # ignore clusters with single element
    clusters_df = clusters_df[clusters_df.duplicated(subset="cluster_id", keep=False)]
    return clusters_df.to_dict(orient="records")
