from collections.abc import Callable

import pendulum
import polars as pl


def filter_services_named_after_structure(
    services_df: pl.DataFrame,
    structures_df: pl.DataFrame,
) -> pl.DataFrame:
    return services_df.join(
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


def filter_services_with_bad_name_length(services_df: pl.DataFrame) -> pl.DataFrame:
    return services_df.filter(
        (pl.col("nom").str.len_chars() < 3) | (pl.col("nom").str.len_chars() > 130)
    )


def int__renommages(
    structures_df: pl.DataFrame,
    services_df: pl.DataFrame,
    rename_fn: Callable[[dict], str | None],
    existing_df: pl.DataFrame | None = None,
) -> pl.DataFrame:
    input_columns = [
        pl.col("nom"),
        pl.col("description"),
        pl.col("thematiques"),
        pl.col("type"),
    ]

    if existing_df is not None and len(existing_df) > 0:
        # incremental run: consider only unseen inputs
        services_df = services_df.join(
            other=existing_df,
            left_on=input_columns,
            right_on=input_columns,
            how="anti",
        )

    # select poorly named services
    services_df = (
        pl.union(
            [
                filter_services_named_after_structure(
                    services_df=services_df,
                    structures_df=structures_df,
                ).with_columns(reason=pl.lit("named_after_structure")),
                filter_services_with_bad_name_length(
                    services_df=services_df,
                ).with_columns(reason=pl.lit("bad_name_length")),
            ]
        )
        .unique(subset="id", keep="first")
        # ensure there is enough information for the renaming function to work properly
        .filter(pl.col("description").str.len_chars() > 100)
    )

    print("Services to rename:")
    print(
        services_df.group_by(
            [pl.col("reason"), pl.col("source")],
        )
        .agg(pl.count())
        .sort(
            by=[
                pl.col("reason"),
                pl.col("count"),
            ],
            descending=True,
        )
    )

    results_df = pl.DataFrame(
        [
            {
                "id": service_data["id"],
                "output": rename_fn(service_data["input"]),
            }
            for service_data in (
                services_df.unique(
                    subset=input_columns,
                    keep="first",
                )
                .with_columns(
                    input=pl.struct(*input_columns),
                )
                .iter_rows(named=True)
            )
        ]
    ).filter(pl.col("output").is_not_null())

    results_df = results_df.join(
        other=services_df,
        on="id",
        how="inner",
    ).select(
        pl.lit(pendulum.now()).alias("generated_at"),
        pl.col("reason"),
        *input_columns,
        pl.col("output"),
    )

    if existing_df is not None and len(existing_df) > 0:
        # incremental run: append results to existing_df
        results_df = pl.concat([existing_df, results_df], how="align")

    return results_df
