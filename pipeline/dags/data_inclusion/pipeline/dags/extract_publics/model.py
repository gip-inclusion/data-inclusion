from collections.abc import Callable

import pendulum
import polars as pl


def validate_services_df(services_df: pl.DataFrame) -> pl.DataFrame:
    expected = pl.Schema(
        {
            "id": pl.String,
            "publics": pl.List(pl.String),
            "publics_precisions": pl.String,
        },
    )

    return services_df.match_to_schema(
        schema=expected,
        extra_columns="ignore",
    )


def validate_extractions_df(extractions_df: pl.DataFrame) -> pl.DataFrame:
    expected = pl.Schema(
        {
            "publics": pl.List(pl.String),
            "publics_precisions": pl.String,
            "output": pl.Object,
            "extracted_at": pl.Datetime(time_zone="UTC"),
            "reason": pl.String,
        },
    )

    return extractions_df.match_to_schema(schema=expected)


def int__publics_extractions(
    services_df: pl.DataFrame,
    extract_fn: Callable[[str], dict | None],
) -> pl.DataFrame | None:
    services_df = validate_services_df(services_df)

    services_df = pl.union(
        [
            services_df.filter(
                pl.col("publics").is_null() & pl.col("publics_precisions").is_not_null()
            ).with_columns(reason=pl.lit("missing_publics")),
            services_df.filter(
                pl.col("publics").list.contains("tous-publics")
                & pl.col("publics_precisions").is_not_null()
            ).with_columns(reason=pl.lit("tous_publics")),
        ]
    ).unique(subset="id", keep="first")

    if services_df.is_empty():
        print("No new data, skipping.")
        return None

    print("Extractions to do:")
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

    # TODO: remove this arbitrary limit
    # There is currently too many services to process, which might be too costly
    # We should implement incremental processing and further filter the services
    # to process. Also use the batch api.
    services_df = services_df.sample(n=200)

    results_df = pl.DataFrame(
        [
            {
                "id": service_data["id"],
                "output": extract_fn(service_data["publics_precisions"]),
            }
            for service_data in (
                services_df.unique(
                    subset=pl.col("publics_precisions"),
                    keep="first",
                ).iter_rows(named=True)
            )
        ],
        infer_schema_length=None,
        schema_overrides={"output": pl.Object},
    ).filter(pl.col("output").is_not_null())

    results_df = results_df.join(
        other=services_df,
        on="id",
        how="inner",
    ).select(
        pl.lit(pendulum.now(tz="Europe/Paris"))
        .alias("extracted_at")
        .dt.convert_time_zone("UTC"),
        pl.col("reason"),
        pl.col("publics"),
        pl.col("publics_precisions"),
        pl.col("output"),
    )

    # TODO: flatten "output" column

    return validate_extractions_df(results_df)
