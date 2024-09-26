import os

import pandas as pd
from sqlalchemy import create_engine
from unidecode import unidecode

QUALITY_SOURCES = [
    "action-logement",
    "dora",
    "emplois-de-linclusion",
    "france-travail",
    "mes-aides",
    "soliguide",
]

engine = create_engine(os.getenv("PGDATABASE"))

df = pd.read_sql_table("api__structures", engine)

df["id"] = df["_di_surrogate_id"]
df["date_maj"] = pd.to_datetime(df["date_maj"]).dt.strftime("%m/%d/%Y")
df["nom"] = df["nom"].str.lower().str.strip().apply(unidecode)
df["location"] = df.apply(
    lambda row: [
        row["latitude"] if pd.notnull(row["latitude"]) else 0.0,
        row["longitude"] if pd.notnull(row["longitude"]) else 0.0,
    ],
    axis=1,
)
df["siren"] = df["siret"].str[:9]
df["telephone"] = df["telephone"].str.replace(" ", "").str.replace(".", "")

filtered_df = df[
    df["source"].isin(QUALITY_SOURCES) & df["code_insee"].notnull() & ~df["antenne"]
]

final_df = filtered_df[
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

final_df.to_sql("preprocessed_structures", engine, if_exists="replace", index=False)
