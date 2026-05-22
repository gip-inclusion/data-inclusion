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


def validate_structures_df(structures_df: pl.DataFrame) -> pl.DataFrame:
    expected = pl.Schema(
        {
            "id": pl.String,
            "nom": pl.String,
        },
    )

    return structures_df.match_to_schema(
        schema=expected,
        extra_columns="ignore",
    )


def validate_services_df(services_df: pl.DataFrame) -> pl.DataFrame:
    expected = pl.Schema(
        {
            "id": pl.String,
            "structure_id": pl.String,
            "nom": pl.String,
            "description": pl.String,
            "thematiques": pl.List(pl.String),
            "type": pl.String,
        },
    )

    return services_df.match_to_schema(
        schema=expected,
        extra_columns="ignore",
    )


def validate_renamings_df(renamings_df: pl.DataFrame) -> pl.DataFrame:
    expected = pl.Schema(
        {
            "reason": pl.String,
            "nom": pl.String,
            "description": pl.String,
            "thematiques": pl.List(pl.String),
            "type": pl.String,
            "output": pl.String,
            "generated_at": pl.Datetime(time_zone="UTC"),
        },
    )

    return renamings_df.match_to_schema(schema=expected)


def int__renommages(
    structures_df: pl.DataFrame,
    services_df: pl.DataFrame,
    rename_fn: Callable[[dict], str | None],
    existing_df: pl.DataFrame | None = None,
) -> pl.DataFrame | None:
    structures_df = validate_structures_df(structures_df)
    services_df = validate_services_df(services_df)
    if existing_df is not None:
        existing_df = validate_renamings_df(existing_df)

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
            nulls_equal=True,
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

    if services_df.is_empty():
        print("No new services to rename, skipping...")
        return None

    print("Renamings to do:")
    print(
        services_df.group_by(pl.col("reason"))
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

    if results_df.is_empty():
        print("Renaming function did not return any new name, skipping...")
        return existing_df if existing_df is not None else None

    results_df = results_df.join(
        other=services_df,
        on="id",
        how="inner",
    ).select(
        pl.lit(pendulum.now(tz="Europe/Paris"))
        .alias("generated_at")
        .dt.convert_time_zone("UTC"),
        pl.col("reason"),
        *input_columns,
        pl.col("output"),
    )

    if existing_df is not None and len(existing_df) > 0:
        # incremental run: append results to existing_df
        results_df = pl.concat([existing_df, results_df], how="align")

    return validate_renamings_df(results_df)
