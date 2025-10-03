#!/usr/bin/env -S uv run --script
# flake8: noqa: E402

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pandas",
#     "openpyxl"
# ]
# ///


import pandas as pd

df = pd.read_excel("ft_asp_structures_dedupe.xlsx")

print(df.head())

valid_siret_mask = df["siret"].notna()

max_cluster = df["cluster_id"].max()
needs_cluster = valid_siret_mask & df["cluster_id"].isna()

if needs_cluster.any():
    siret_counts = df.loc[valid_siret_mask, "siret"].value_counts()
    sirets_to_cluster = siret_counts[siret_counts >= 2].index.tolist()
    sirets_needing_assignment = df.loc[
        needs_cluster & df["siret"].isin(sirets_to_cluster), "siret"
    ].unique()
    if len(sirets_needing_assignment) > 0:
        # Find the maximum existing cluster_id
        max_cluster = df["cluster_id"].max()
        start_id = max(765, int(max_cluster) + 1 if pd.notna(max_cluster) else 765)
        siret_to_cluster = {
            siret: start_id + i for i, siret in enumerate(sirets_needing_assignment)
        }
        for siret, cluster_id in siret_to_cluster.items():
            mask = valid_siret_mask & (df["siret"] == siret)
            df.loc[mask, "cluster_id"] = cluster_id


def _transform(x):
    try:
        return f"{int(x)}"
    except ValueError:
        return ""


df["siret"] = df["siret"].apply(_transform)
df["siren"] = df["siren"].apply(_transform)
df["code_postal"] = df["code_postal"].apply(_transform)
df["code_insee"] = df["code_insee"].astype(str)
df["cluster_id"] = df["cluster_id"].apply(_transform).astype(str).str.zfill(5)

df.sort_values(by=["cluster_id", "code_insee"], na_position="last", inplace=True)
df.to_excel("ft_asp_structures_dedupe_with_siret_clusters.xlsx", index=False)
