import numpy as np
import pandas as pd
from src.data_inclusion import processings

df = pd.read_csv(
    "nexus/structures_if.csv", on_bad_lines="warn", encoding_errors="replace", sep=";"
)

df["siret"] = df["siret"].astype(str)
df["siren"] = df["siret"].str[:9]

df["id"] = "if-" + df["siret"]
df["nom"] = df["name"].astype(str)
df["adresse"] = df["street_number_and_address"].astype(str)
df["code_postal"] = df["post_code"].astype(str)
df["commune"] = df["city"].astype(str)
df["date_maj"] = pd.to_datetime(df["update_date"]).dt.tz_localize(None)
df["antenne"] = False
df["courriel"] = None
df["telephone"] = None
df["code_insee"] = None
df["source"] = "immersion-facilitee"

geocoded_df = processings.geocode(df.to_dict(orient="records"))

result_df = geocoded_df.copy()

column_pairs = [
    ("adresse", "result_name"),
    ("code_postal", "result_postcode"),
    ("commune", "result_city"),
    ("code_insee", "result_citycode"),
]

for target_col, source_col in column_pairs:
    result_df[target_col] = np.where(
        geocoded_df["result_score"].astype(float) > 0.8,
        geocoded_df[source_col],
        geocoded_df[target_col],
    )

output_df = result_df[
    [
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

output_df.to_excel("nexus/structures_if.xlsx", index=False)
