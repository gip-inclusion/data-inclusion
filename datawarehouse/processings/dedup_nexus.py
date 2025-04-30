import pandas as pd
import sqlalchemy as sa
from src.data_inclusion import processings

engine = sa.create_engine(
    "postgresql://data-inclusion:data-inclusion@172.17.0.1:5455/data-inclusion"
)

df = pd.read_sql(
    "SELECT * FROM public_intermediate.int__union_structures__enhanced", engine
)
df = df.loc[
    df["source"].isin(
        [
            "dora",
            "emplois-de-linclusion",
        ]
    )
]

df_if = pd.read_excel("nexus/structures_if.xlsx")
df_marche = pd.read_excel("nexus/structures_marche.xlsx")

df_total = pd.concat([df, df_if, df_marche], axis=0, ignore_index=True)

df_total["_di_surrogate_id"] = df_total["id"]
df_total["siret"] = df_total["siret"].astype(str)
df_total["code_postal"] = df_total["code_postal"].astype(str)
df_total["date_maj"] = pd.to_datetime(df_total["date_maj"], format="mixed")
df_total["telephone"] = df_total["telephone"].astype(str)
df_total["code_insee"] = df_total["code_insee"].astype(str)


df_doublons = processings.deduplicate(df_total.to_dict(orient="records"))


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

df.to_excel("nexus/structures_dedupe.xlsx", index=False)
