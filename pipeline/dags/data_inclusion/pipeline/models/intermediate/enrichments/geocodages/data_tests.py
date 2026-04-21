import narwhals as nw
import pointblank as pb
import polars as pl

from data_inclusion.pipeline.models.utils import data_tests


def int__geocodages(
    data: pl.DataFrame,
    adresses_df: pl.DataFrame,
) -> None:
    validation = pb.Validate(data=data)

    # check that all string columns are non-empty strings
    validation.col_vals_regex(
        columns=nw.selectors.string(),
        pattern=r"(.|\s)*\S(.|\s)*",
        na_pass=True,
    )

    validation.col_exists("geocoded_at")
    validation.col_vals_not_null("geocoded_at")

    validation.col_exists("adresse_id")
    validation.specially(
        expr=lambda df: data_tests.relationships(
            left=df,
            left_on=pl.col("adresse_id"),
            right=adresses_df,
            right_on=pl.col("id"),
        )
    )

    validation.col_exists("input_adresse")
    validation.col_exists("input_code_insee")
    validation.col_exists("input_code_postal")
    validation.col_exists("input_commune")
    validation.col_exists("commune")
    validation.col_exists("adresse")
    validation.col_exists("code_postal")
    validation.col_exists("code_commune")

    validation.col_exists("code_arrondissement")
    validation.col_exists("latitude")
    validation.col_exists("longitude")
    validation.col_exists("score")

    validation.col_exists("type")
    validation.col_vals_in_set(
        "type",
        set=[
            "housenumber",
            "street",
            "locality",
            "municipality",
        ],
    )

    data_tests.assert_passing(validation)
