#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "sqlalchemy",
#     "pandas",
#     "phonenumbers",
#     "dedupe",
#     "dedupe-variable-datetime",
#     "unidecode",
#     "psycopg2-binary",
#     "openpyxl",
#     "xlsxwriter"
# ]
# ///

import sys
from pathlib import Path

# Add the src directory to Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

import pandas as pd
import sqlalchemy as sa

from src.data_inclusion.processings import deduplicate

engine = sa.create_engine(
    "postgresql://data-inclusion:data-inclusion@172.17.0.1:5455/data-inclusion"
)

df_mba = pd.read_sql(
    """
    SELECT * FROM public_intermediate.int__structures
    WHERE source = 'ma-boussole-aidants' AND typologie = 'CCAS'
    """,
    engine,
)

print(df_mba.head())

df_ft = pd.read_sql(
    """
    SELECT
        DISTINCT ON (1)
        e.siret AS id,
        'sirene' AS source,
        e."dateDernierTraitementEtablissement" AS date_maj,
        l."denominationUniteLegale" AS nom,
        e."libelleCommuneEtablissement" AS commune,
        TRIM(CONCAT(
            COALESCE(e."numeroVoieEtablissement", ''),
            COALESCE(e."indiceRepetitionEtablissement", ''),
            ' ',
            COALESCE(e."typeVoieEtablissement", ''),
            ' ',
            COALESCE(e."libelleVoieEtablissement", '')
        )) AS adresse,
        e."codePostalEtablissement" AS code_postal,
        e."codeCommuneEtablissement" AS code_insee,
        e.siret AS siret,
        l.siren AS siren,
        s.latitude AS latitude,
        s.longitude AS longitude,
        s.telephone AS telephone,
        s.courriel AS courriel
    FROM sirene.stock_unite_legale AS l
    LEFT JOIN sirene.stock_etablissement AS e ON l.siren = e.siren
    LEFT JOIN public_intermediate.int__structures AS s ON s.siret = e.siret
    WHERE (
        l."categorieJuridiqueUniteLegale" = '7361'
        OR
        l."categorieJuridiqueUniteLegale" = '7367'
    )
    AND l."etatAdministratifUniteLegale" = 'A'
    AND e."etatAdministratifEtablissement" = 'A'
    AND e."etablissementSiege" = 'true'
    """,
    engine,
)
print(df_ft.head())

df_total = pd.concat([df_mba, df_ft], axis=0, ignore_index=True)

df_total["_di_surrogate_id"] = df_total["id"]
df_total["siret"] = df_total["siret"].astype(str)
df_total["code_postal"] = df_total["code_postal"].astype(str)
df_total["date_maj"] = pd.to_datetime(df_total["date_maj"], format="mixed")
df_total["telephone"] = df_total["telephone"].astype(str)
df_total["courriel"] = df_total["courriel"].astype(str)
df_total["code_insee"] = df_total["code_insee"].astype(str)

# stop any variability about the name, all CCAS are named "CCAS"
df_total["nom"] = "CCAS"

df_total = df_total.assign(
    adresse=df_total["adresse"].str.lower().str.strip(),
    commune=df_total["commune"].str.lower().str.strip(),
    nom=df_total["nom"].str.lower().str.strip(),
    courriel=df_total["courriel"].str.lower().str.strip(),
)

df_doublons = deduplicate(df_total.to_dict(orient="records"))


df = df_total.merge(
    df_doublons,
    how="left",
    left_on="_di_surrogate_id",
    right_on="structure_id",
    suffixes=("", "_dedup"),
)

df = df[
    [
        "cluster_id",
        "score",
        "id",
        "source",
        "date_maj",
        "nom",
        "commune",
        "adresse",
        "code_postal",
        "code_insee",
        "siret",
        "siren",
        "telephone",
        "courriel",
        "latitude",
        "longitude",
    ]
]


def is_not_empty(value):
    if pd.isna(value):
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    return True


df["has_cluster_id"] = df["cluster_id"].apply(is_not_empty)

summary = df.groupby("source").agg(
    total_rows=("id", "count"), rows_with_cluster_id=("has_cluster_id", "sum")
)

summary["percent_with_cluster_id"] = (
    summary["rows_with_cluster_id"] / summary["total_rows"] * 100
).round(2)

total_with_cluster_id = summary["rows_with_cluster_id"].sum()
summary["percent_of_total_clustered"] = (
    summary["rows_with_cluster_id"] / total_with_cluster_id * 100
).round(2)

totals = pd.DataFrame(
    {
        "total_rows": [df.shape[0]],
        "rows_with_cluster_id": [total_with_cluster_id],
        "percent_with_cluster_id": [
            (total_with_cluster_id / df.shape[0] * 100).round(2)
        ],
        "percent_of_total_clustered": [100.0],
    },
    index=["TOTAL"],
)
final_summary = pd.concat([summary, totals])

print("Summary of Cluster ID Distribution by Source:")
print(final_summary)

df.to_excel("mba_sirene_structures_dedupe.xlsx", index=False, engine="xlsxwriter")
