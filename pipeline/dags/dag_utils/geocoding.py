import csv
import io
import logging

import numpy as np
import pandas as pd
import requests
import tenacity
from tenacity import before, stop, wait

logger = logging.getLogger(__name__)


class GeocodingBackend:
    def geocode(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError


class BaseAdresseNationaleBackend(GeocodingBackend):
    def __init__(self, base_url: str):
        self.base_url = base_url.strip("/")

    def _geocode(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Will send address batch, dimensions=%s", df.shape)
        with io.BytesIO() as buf:
            df.to_csv(buf, index=False, quoting=csv.QUOTE_ALL, sep="|")

            try:
                response = requests.post(
                    self.base_url + "/search/csv/",
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
            # In some cases (ex: address='20' and city='Paris'), the BAN API will return
            # a municipality as a result with a very high score. This is be discarded
            # as this will not be valuable information to localize a structure.
            results_df = results_df[results_df.result_type != "municipality"]

        logger.info("Got result for address batch, dimensions=%s", results_df.shape)
        return results_df

    def geocode(self, df: pd.DataFrame) -> pd.DataFrame:
        # BAN api limits the batch geocoding to 50MB of data
        # In our tests, 10_000 rows is about 1MB; but we'll be conservative
        # since we also want to avoid upload timeouts.
        BATCH_SIZE = 20_000

        # drop rows that have not at least one commune, code_insee or code_postal
        # as the result can't make sense.
        # Note that we keep the rows without an address, as they can be used to
        # at least resolve the city.
        df = df.dropna(subset=["code_postal", "code_insee", "commune"], thresh=2)
        df = df.sort_values(by="_di_surrogate_id")
        # Cleanup the values a bit to help the BAN's scoring. After experimentation,
        # looking for "Ville-Nouvelle" returns worse results than "Ville Nouvelle",
        # probably due to a tokenization in the BAN that favors spaces.
        # In the same fashion, looking for "U.R.S.S." returns worse results than using
        # "URSS" for "Avenue de l'U.R.S.S.". With the dots, it does not find the
        # street at all ¯\_(ツ)_/¯
        df = df.assign(
            adresse=(df.adresse.str.strip().replace("-", " ").replace(".", "")),
            commune=df.commune.str.strip(),
        )

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
                            # arbitrary threshold, if less than this percentage of
                            # the rows have been geocoded, retry.
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
            np.arange(len(df)) // BATCH_SIZE,
            group_keys=False,
        ).apply(_geocode_with_retry)
