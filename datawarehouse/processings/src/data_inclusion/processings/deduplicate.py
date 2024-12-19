import json
import logging
from dataclasses import dataclass

import dedupe
import pandas as pd
import phonenumbers
import requests
from unidecode import unidecode

THRESHOLD = 0.75  # as defined in our Jupyter simulations
MODEL_URL = (
    "https://github.com/gip-inclusion/data-inclusion"
    "/raw/042ca760bd1f249d81bb2df622413d8be744c7b3/deduplication/model.bin"
)


logger = logging.getLogger(__name__)


@dataclass
class DeduplicateInput:
    _di_surrogate_id: str
    adresse: str
    antenne: bool
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


def _download_file(url: str, filename: str):
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    with open(filename, "wb") as f:
        f.write(response.content)


def deduplicate(data: list[DeduplicateInput]) -> pd.DataFrame:
    df = pd.DataFrame.from_records(data)

    # focus on "quality" sources
    df = df.loc[
        df["source"].isin(
            [
                "action-logement",
                "dora",
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

    # ignore antennes
    df = df.loc[df["antenne"] != True]  # noqa: E712

    # ignore structures with no city code (no address, no geolocation)
    df = df.loc[df["code_insee"].notnull()]

    # some cleanups and formatting
    df["id"] = df["_di_surrogate_id"]
    df["date_maj"] = pd.to_datetime(df["date_maj"]).dt.strftime("%m/%d/%Y")
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

    logger.info(f"reading training data from {MODEL_URL=}")
    _download_file(MODEL_URL, "/tmp/model.bin")

    with open("/tmp/model.bin", "rb") as f:
        deduper = dedupe.StaticDedupe(f)

    data = json.loads(df.to_json(orient="records"))

    logger.info(f"partitioning data into clusters with {THRESHOLD=}")
    clustered_dupes = deduper.partition(
        data={d["id"]: d for d in data},
        threshold=THRESHOLD,
    )

    logger.info("exporting clusters to JSON")
    output_df = pd.DataFrame(columns=["id", "structure_id", "score", "size"])
    cluster_id = 0
    for cluster, scores in clustered_dupes:
        if len(cluster) > 1:
            for structure_id, score in zip(cluster, scores):
                output_df.loc[len(output_df)] = [
                    cluster_id,
                    structure_id,
                    score,
                    len(cluster),
                ]
            cluster_id += 1

    return output_df.to_dict(orient="records")
