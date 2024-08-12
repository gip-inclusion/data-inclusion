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
                "https://api-adresse.data.gouv.fr/search/csv/",
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
                    "postcode": "code_postal",
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

    logger.info("Got result for address batch, dimensions=%s", results_df.shape)
    return results_df


@dataclass
class GeocodeInput:
    id: str
    adresse: str
    code_insee: str
    code_postal: str
    commune: str


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

    # Cleanup the values a bit to help the BAN's scoring. After experimentation,
    # looking for "Ville-Nouvelle" returns worse results than "Ville Nouvelle",
    # probably due to a tokenization in the BAN that favors spaces.
    # In the same fashion, looking for "U.R.S.S." returns worse results than using
    # "URSS" for "Avenue de l'U.R.S.S.". With the dots, it does not find the
    # street at all ¯\_(ツ)_/¯
    df = df.assign(
        adresse=df.adresse.str.strip().replace("-", " ").replace(".", ""),
        commune=df.commune.str.strip(),
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
