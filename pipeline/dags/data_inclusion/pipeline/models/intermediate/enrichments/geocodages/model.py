import re
from collections.abc import Callable
from typing import Concatenate

import pendulum
import polars as pl

from data_inclusion.pipeline.models.intermediate.enrichments.geocodages import (
    geoplateforme,
)

type GeocoderFunc[**P] = Callable[
    Concatenate[list[dict], P], list[geoplateforme.GeocodeCsvResponseRow]
]


STREET_ABBREVIATIONS = {
    r"(?i) all ": " allée ",
    r"(?i) av ": " avenue ",
    r"(?i) bd ": " boulevard ",
    r"(?i) bvd ": " boulevard ",
    r"(?i) che ": " chemin ",
    r"(?i) crs ": " cours ",
    r"(?i) esp ": " esplanade ",
    r"(?i) imp ": " impasse ",
    r"(?i) pl ": " place ",
    r"(?i) pro ": " promenade ",
    r"(?i) rd pt ": " rond point ",
    r"(?i) rte ": " route ",
    r"(?i) sq ": " square ",
}


def preprocess_adresse(adresse: str) -> str:
    adresse = adresse.replace("-", " ").replace(".", "")
    adresse = re.sub(r"(?i)BP *[0-9]*", "", adresse)
    adresse = re.sub(r"(?i)CS *[0-9]*", "", adresse)

    for pattern, replacement in STREET_ABBREVIATIONS.items():
        adresse = re.sub(pattern, replacement, adresse)

    adresse = adresse.strip(" -")
    return adresse


def preprocess_commune(commune: str) -> str:
    commune = re.sub(r"(?i)cedex.*", "", commune)
    commune = commune.strip(" -")
    return commune


def postprocess_citycode(citycode: str) -> str:
    for prefix_arrondissement, code_commune in [
        ("751", "75056"),
        ("132", "13055"),
        ("693", "69123"),
    ]:
        if citycode.startswith(prefix_arrondissement):
            return code_commune
    return citycode


adresses_schema = pl.Schema({
    "id": pl.String,
    "adresse": pl.String,
    "code_postal": pl.String,
    "code_insee": pl.String,
    "commune": pl.String,
})

communes_schema = pl.Schema({
    "code": pl.String,
    "nom": pl.String,
    "codes_postaux": pl.List(pl.String),
})

geocodages_schema = pl.Schema({
    "adresse_id": pl.String,
    "input_adresse": pl.String,
    "input_code_postal": pl.String,
    "input_code_insee": pl.String,
    "input_commune": pl.String,
    "latitude": pl.Float64,
    "longitude": pl.Float64,
    "commune": pl.String,
    "adresse": pl.String,
    "code_postal": pl.String,
    "score": pl.Float64,
    "type": pl.String,
    "geocoded_at": pl.Datetime(time_zone="Europe/Paris"),
    "code_commune": pl.String,
    "code_arrondissement": pl.String,
})


def int__geocodages(
    adresses_df: pl.DataFrame,
    communes_df: pl.DataFrame,
    existing_df: pl.DataFrame | None = None,
    geocode: GeocoderFunc = geoplateforme.geocode,
) -> pl.DataFrame:
    adresses_df = adresses_df.match_to_schema(adresses_schema)
    communes_df = communes_df.match_to_schema(communes_schema)
    existing_df = (
        existing_df.match_to_schema(geocodages_schema)
        if existing_df is not None
        else None
    )

    mode = (
        "incremental"
        if existing_df is not None and len(existing_df) > 0
        else "full-refresh"
    )

    if mode == "incremental":
        inputs_df = (
            adresses_df
            .join(
                other=existing_df.select([
                    "adresse_id",
                    "input_adresse",
                    "input_code_postal",
                    "input_code_insee",
                    "input_commune",
                ]),
                left_on="id",
                right_on="adresse_id",
                how="left",
                coalesce=False,
            )
            .filter(
                # new rows (not yet geocoded)
                pl.col("adresse_id").is_null()
                # changed rows
                | (
                    (pl.col("adresse") != pl.col("input_adresse"))
                    | (pl.col("code_postal") != pl.col("input_code_postal"))
                    | (pl.col("code_insee") != pl.col("input_code_insee"))
                    | (pl.col("commune") != pl.col("input_commune"))
                )
            )
            .drop(
                "adresse_id",
                "input_adresse",
                "input_code_postal",
                "input_code_insee",
                "input_commune",
            )
        )
    else:
        inputs_df = adresses_df

    inputs_df = (
        inputs_df
        # drop unknown postal codes
        .join(
            (
                communes_df
                .explode("codes_postaux")
                .select(pl.col("codes_postaux").alias("code_postal"))
                .unique()
            ),
            on="code_postal",
            how="left",
            coalesce=False,
        )
        .select(pl.exclude("code_postal"), code_postal="code_postal_right")
        # preprocess
        .select(
            "id",
            "code_insee",
            "code_postal",
            adresse=pl.col("adresse").map_elements(preprocess_adresse),
            commune=pl.col("commune").map_elements(preprocess_commune),
        )
        # drop rows with too many missing values
        .filter(
            pl.sum_horizontal(
                pl.col("code_postal").is_null(),
                pl.col("code_insee").is_null(),
                pl.col("commune").is_null(),
            )
            <= 1
        )
    )

    # geocode with geoplateforme
    geocode_results = geocode(
        inputs_df.to_dicts(),
        columns=["adresse", "code_postal", "commune"],
        filters={"citycode": "code_insee"},
    )

    results_df = (
        pl
        .from_records(
            data=geocode_results,
            schema={
                "result_status": pl.String,
                "id": pl.String,
                "latitude": pl.Float64,
                "longitude": pl.Float64,
                "result_score": pl.Float64,
                "result_type": pl.String,
                "result_name": pl.String,
                "result_postcode": pl.String,
                "result_city": pl.String,
                "result_citycode": pl.String,
                "result_district": pl.String,
            },
        )
        .join(other=inputs_df, left_on="id", right_on="id", how="inner")
        .filter(pl.col("result_status") == pl.lit("ok"))
        .filter(
            (pl.col("result_type") != pl.lit("municipality"))
            | pl.col("adresse").is_null()
            | pl.col("adresse").str.to_lowercase().str.contains(r"bourg|village|mairie")
            | pl
            .col("commune")
            .str.to_lowercase()
            .str.contains(pl.col("adresse").str.to_lowercase())
            | pl
            .col("adresse")
            .str.to_lowercase()
            .str.contains(pl.col("commune").str.to_lowercase())
        )
        .select(
            latitude="latitude",
            longitude="longitude",
            input_adresse="adresse",
            input_code_insee="code_insee",
            input_code_postal="code_postal",
            input_commune="commune",
            adresse_id="id",
            commune="result_city",
            adresse="result_name",
            code_postal="result_postcode",
            score="result_score",
            type="result_type",
            geocoded_at=pl.lit(pendulum.now()),
            code_commune=pl.col("result_citycode").map_elements(postprocess_citycode),
            code_arrondissement=pl
            .when(
                pl.col("result_citycode").str.starts_with("751")
                | pl.col("result_citycode").str.starts_with("132")
                | pl.col("result_citycode").str.starts_with("693")
            )
            .then(pl.lit("result_citycode"))
            .otherwise(None),
        )
    )

    if mode == "incremental":
        # merge with existing geocodages, keeping the most recent geocoding
        results_df = (
            pl
            .concat([results_df, existing_df], how="align")
            .sort(
                by="geocoded_at",
                descending=True,
            )
            .unique(
                subset="adresse_id",
                keep="first",
            )
        )

    return results_df
