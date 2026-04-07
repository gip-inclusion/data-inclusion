import csv
import io
import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
import requests

logger = logging.getLogger(__name__)


def _geocode(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Will send address batch, dimensions=%s", df.shape)
    with io.BytesIO() as buf:
        df.to_csv(buf, index=False, quoting=csv.QUOTE_ALL, sep="|")

        try:
            response = requests.post(
                "https://data.geopf.fr/geocodage/search/csv/",
                files={"data": ("data.csv", buf.getvalue(), "text/csv")},
                data={
                    "columns": ["adresse", "code_postal", "commune"],
                    # Post-filter on the INSEE code and not the zipcode.
                    # Explanations from the BAN API creators:
                    # The postcode is problematic for cities with multiple zipcodes
                    # if the supplied zipcode is wrong, or the one in the BAN is.
                    # The INSEE code is more stable, unique and reliable.
                    # Also this post-filter does not return "possible" results,
                    # it blindly filters-out.
                    "citycode": "code_insee",
                },
                timeout=180,  # we upload 2MB of data, so we need a high timeout
            )
            response.raise_for_status()
        except requests.RequestException as e:
            logger.info("Error while fetching `%s`: %s", e.request.url, e)
            return pd.DataFrame()

    with io.StringIO() as f:
        f.write(response.text)
        f.seek(0)
        results_df = pd.read_csv(
            f,
            encoding_errors="replace",
            on_bad_lines="warn",
            dtype=str,
            sep="|",
        )
        results_df = results_df.replace({np.nan: None})

        results_df = results_df[
            # only keep results not being "only a city"
            (results_df.result_type != "municipality")
            # ... unless the supplied address was empty
            | (results_df.adresse.isna())
            | (results_df.adresse == "")
            # ... or the street address is "bourg", "mairie" or "village"
            | (
                results_df.adresse.str.contains(
                    "bourg|village|mairie", case=False, regex=True
                )
            )
            # ... or the address is more or less the same as the city name
            # (e.g: adresse=Anjou and commune=anjou 07300)
            | results_df.apply(
                lambda row: str(row["commune"]).lower() in str(row["adresse"]).lower(),
                axis=1,
            )
            | results_df.apply(
                lambda row: str(row["adresse"]).lower() in str(row["commune"]).lower(),
                axis=1,
            )
        ]

    logger.info("Got result for address batch, dimensions=%s", results_df.shape)
    return results_df


@dataclass
class GeocodeInput:
    id: str
    adresse: str
    code_insee: str
    code_postal: str
    commune: str


# abbreviations found in the FINESS "typvoie" column
STREET_ABBREVIATIONS = {
    "all": "allée",
    "av": "avenue",
    "bd": "boulevard",
    "bld": "boulevard",
    "bvd": "boulevard",
    "che": "chemin",
    "chem": "chemin",
    "crs": "cours",
    "esp": "esplanade",
    "fg": "faubourg",
    "ham": "hameau",
    "imp": "impasse",
    "ld": "lieu-dit",
    "mte": "montée",
    "pas": "passage",
    "pl": "place",
    "pro": "promenade",
    "prom": "promenade",
    "pte": "porte",
    "qu": "quai",
    "qua": "quartier",
    "r": "rue",
    "rd pt": "rond point",
    "rle": "ruelle",
    "rpt": "rond point",
    "rte": "route",
    "sq": "square",
    "tra": "traverse",
    "voi": "voie",
}


def get_street_abbreviations() -> dict[str, str]:
    return {rf"(?i)\b{abbrev}\b": full for abbrev, full in STREET_ABBREVIATIONS.items()}


def geocode(
    data: GeocodeInput | list[GeocodeInput],
    batch_size: int = 20_000,
) -> pd.DataFrame:
    # BAN api limits the batch geocoding to 50MB of data
    # In our tests, 10_000 rows is about 1MB; but we'll be conservative
    # since we also want to avoid upload timeouts.

    data = data if isinstance(data, list) else [data]
    df = pd.DataFrame.from_records(data)

    # drop rows that have not at least one commune, code_insee or code_postal
    # as the result can't make sense.
    # Note that we keep the rows without an address, as they can be used to
    # at least resolve the city.
    df = df.dropna(subset=["code_postal", "code_insee", "commune"], thresh=2)

    # Cleanup/rewrite the values to help the BAN's scoring.
    df["adresse"] = (
        df["adresse"]
        .str.strip(" -")
        # After experimentation, looking for "Ville-Nouvelle" returns
        # worse results than "Ville Nouvelle", probably due to a tokenization in
        # the BAN that favors spaces.
        # In the same fashion, looking for "U.R.S.S." returns worse results than
        # using "URSS" for "Avenue de l'U.R.S.S.". With the dots, it does not
        # find the street at all ¯\_(ツ)_/¯
        .replace("-", " ")
        .replace(".", "")
    )
    df["adresse_original"] = df["adresse"]
    df["adresse"] = (
        df["adresse"]
        .str.replace(r"(?i)BP *[0-9]*", "", regex=True)
        .replace(r"(?i)CS *[0-9]*", "", regex=True)
    )
    # We use (?i) to make the regex case-insensitive.
    # We can remove everything after cedex, it is always at the end of the string.
    df["commune_original"] = df["commune"]
    df["commune"] = df["commune"].str.replace(r"(?i)cedex.*", "", regex=True)

    df["code_postal"] = np.where(
        ~df["adresse"].eq(df["adresse_original"]), None, df["code_postal"]
    )
    df["code_postal"] = np.where(
        ~df["commune"].eq(df["commune_original"]), None, df["code_postal"]
    )

    df["commune"] = df["commune"].apply(lambda x: x.strip(" -") if x else x)
    df["adresse"] = (
        df["adresse"]
        .replace(get_street_abbreviations(), regex=True)
        .apply(lambda x: x.strip(" -") if x else x)
    )

    logger.info(f"Only {len(df)} rows can be geocoded.")

    return (
        df.groupby(
            np.arange(len(df)) // batch_size,
            group_keys=False,
        )
        .apply(_geocode)
        .to_dict(orient="records")
    )
