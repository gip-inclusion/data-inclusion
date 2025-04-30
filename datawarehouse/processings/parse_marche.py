import numpy as np
import pandas as pd
from src.data_inclusion import processings

month_map = {
    "janv.": 1,
    "janvier": 1,
    "jan.": 1,
    "jan": 1,
    "févr.": 2,
    "février": 2,
    "fév.": 2,
    "fev.": 2,
    "fév": 2,
    "fev": 2,
    "mars": 3,
    "mar.": 3,
    "mar": 3,
    "avr.": 4,
    "avril": 4,
    "avr": 4,
    "mai": 5,
    "mai.": 5,
    "juin": 6,
    "juin.": 6,
    "juil.": 7,
    "juillet": 7,
    "juil": 7,
    "août": 8,
    "aout": 8,
    "août.": 8,
    "aout.": 8,
    "sept.": 9,
    "septembre": 9,
    "sept": 9,
    "sep.": 9,
    "sep": 9,
    "oct.": 10,
    "octobre": 10,
    "oct": 10,
    "nov.": 11,
    "novembre": 11,
    "nov": 11,
    "déc.": 12,
    "décembre": 12,
    "dec.": 12,
    "déc": 12,
    "dec": 12,
}


def parse_french_date(date_str):
    try:
        parts = date_str.replace(",", "").split()
        day = int(parts[0])
        month_str = parts[1].lower().rstrip(".,")
        if month_str.endswith("."):
            month_str = month_str[:-1]
        month = month_map.get(month_str + ".")
        if month is None:
            month = month_map.get(month_str)

        year = int(parts[2])
        time_parts = parts[3].split(":")
        hour = int(time_parts[0])
        minute = int(time_parts[1])

        return pd.Timestamp(year, month, day, hour, minute)
    except Exception as e:
        print(f"Error parsing {date_str}: {e}")
        return None


df = pd.read_csv(
    "nexus/structures_marche.csv",
    on_bad_lines="warn",
    encoding_errors="replace",
    sep=",",
)

df["siret"] = df["Siret"].astype(str)
df["siren"] = df["siret"].str[:9]

df["id"] = "marche-" + df["ID"].astype(str)
df["nom"] = df["Name"].astype(str)
df["adresse"] = df["Address"].astype(str)
df["code_postal"] = df["Post Code"].astype(str)
df["commune"] = df["City"].astype(str)
df["date_maj"] = df["Updated At"].apply(parse_french_date).dt.tz_localize(None)
df["courriel"] = df["Email"].astype(str)
df["telephone"] = df["Phone"].astype(str)
df["code_insee"] = None
df["antenne"] = False
df["source"] = "le-marche"

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

output_df.to_excel("nexus/structures_marche.xlsx", index=False)
