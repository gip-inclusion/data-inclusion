from collections.abc import Callable

import polars as pl


def int__renommages(
    structures_df: pl.DataFrame,
    services_df: pl.DataFrame,
    rename_fn: Callable[[dict], str | None],
) -> pl.DataFrame:
    # services named after their structure
    services_df = services_df.join(
        other=structures_df,
        left_on=[
            pl.col("structure_id"),
            pl.col("nom").str.to_lowercase(),
        ],
        right_on=[
            pl.col("id"),
            pl.col("nom").str.to_lowercase(),
        ],
        how="semi",
    )

    services_df = services_df.with_columns(
        generated_nom=pl.struct(pl.all()).map_elements(
            function=rename_fn,
            return_dtype=pl.String,
        )
    )

    return services_df
