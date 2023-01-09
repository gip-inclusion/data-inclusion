import csv
import io
import logging

import numpy as np
import pandas as pd
import requests
import tenacity
from tenacity import before, stop, wait

from data_inclusion.scripts.tasks import utils

logger = logging.getLogger(__name__)


class GeocodingBackend:
    def geocode(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError


class BaseAdresseNationaleBackend(GeocodingBackend):
    def __init__(self, base_url: str):
        self.base_url = base_url.strip("/")

    def _geocode(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[["id", "adresse", "code_postal", "commune"]]

        with io.BytesIO() as buf:
            df.to_csv(buf, index=False, quoting=csv.QUOTE_ALL, sep="|")

            try:
                response = requests.post(
                    self.base_url + "/search/csv/",
                    files={"data": ("data.csv", buf.getvalue(), "text/csv")},
                    data={
                        "columns": ["adresse", "code_postal", "commune"],
                        "postcode": "code_postal",
                    },
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

        return results_df

    def geocode(self, df: pd.DataFrame) -> pd.DataFrame:
        # BAN api limits the batch geocoding to 1000 entries per batch
        batch_size = 1000

        # drop rows with missing input values
        # if not done, the BAN api will fail the entire batch
        df = df.dropna(subset=["adresse", "code_postal", "commune"], how="all")
        df = df.sort_values(by="id")
        df = df.assign(adresse=df.adresse.str.replace("-", " "))

        logger.info(f"Only {len(df)} rows can be geocoded.")

        def _geocode_with_retry(df: pd.DataFrame) -> pd.DataFrame:
            try:
                for attempt in tenacity.Retrying(
                    stop=stop.stop_after_attempt(6),
                    wait=wait.wait_random_exponential(multiplier=10),
                    before=before.before_log(logger, logging.INFO),
                ):
                    with attempt:
                        results_df = self._geocode(df)

                        if (
                            len(df) > 0
                            and len(results_df.dropna(subset=["result_citycode"]))
                            / len(df)
                            < 0.3
                        ):
                            # the BAN api often fails to properly complete a
                            # geocoding batch. If that happens, raise for retry.
                            raise tenacity.TryAgain

                        return results_df

            except tenacity.RetryError:
                logger.error("Failed to geocode batch")
                return results_df

        return df.groupby(
            np.arange(len(df)) // batch_size,
            group_keys=False,
        ).apply(_geocode_with_retry)


def geocode_data(
    df: pd.DataFrame,
    geocoding_backend: GeocodingBackend,
) -> pd.DataFrame:
    df = utils.deserialize_df_data(df)

    utils.log_df_info(df, logger)
    results_df = geocoding_backend.geocode(df)
    utils.log_df_info(results_df, logger)

    return results_df
