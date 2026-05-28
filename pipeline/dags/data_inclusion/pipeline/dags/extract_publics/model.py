import uuid
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


def validate_profils_df(profils_df: pl.DataFrame) -> pl.DataFrame:
    expected = pl.Schema(
        {
            "extraction_id": pl.String,
            "extracted_at": pl.Datetime(time_zone="UTC"),
            "reason": pl.String,
            "profils_total": pl.Int64,
            "profils_number": pl.Int64,
            "input__publics": pl.List(pl.String),
            "input__publics_precisions": pl.String,
            "output__age__max": pl.Int64,
            "output__age__min": pl.Int64,
            "output__age": pl.String,
            "output__genre": pl.String,
            "output__activite": pl.String,
            "output__allocation": pl.String,
            "output__statut_administratif": pl.String,
            "output__lieu_residence": pl.String,
            "output__handicap": pl.Boolean,
            "output__famille": pl.Boolean,
            "output__adherent": pl.Boolean,
        },
    )

    return profils_df.match_to_schema(schema=expected)


def int__extracted_profiles(
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
        .agg(pl.len())
        .sort(
            by=[
                pl.col("reason"),
                pl.col("len"),
            ],
            descending=True,
        )
    )

    # TODO: remove this arbitrary limit
    # There is currently too many services to process, which might be too costly
    # We should implement incremental processing and further filter the services
    # to process. Also use the batch api.
    if len(services_df) > 100:
        services_df = services_df.sample(n=100, shuffle=True)

    profils = []
    for service_data in services_df.unique(
        subset=pl.col("publics_precisions"),
        keep="first",
    ).iter_rows(named=True):
        extraction_id = str(uuid.uuid4())
        extraction_data = extract_fn(service_data["publics_precisions"])

        if extraction_data is None or extraction_data["profils"] is None:
            continue

        for profil_number, profil_data in enumerate(extraction_data["profils"]):
            if "age" in profil_data and isinstance(profil_data["age"], dict):
                age = profil_data.pop("age")
                if "min" in age:
                    profil_data["age__min"] = age["min"]
                if "max" in age:
                    profil_data["age__max"] = age["max"]

            profils.append(
                {
                    "extraction_id": extraction_id,
                    "profils_total": len(extraction_data["profils"]),
                    "profils_number": profil_number,
                    "input__publics": service_data["publics"],
                    "input__publics_precisions": service_data["publics_precisions"],
                    "reason": service_data["reason"],
                    "output__age": profil_data.get("age"),
                    "output__age__max": profil_data.get("age__max"),
                    "output__age__min": profil_data.get("age__min"),
                    "output__genre": profil_data.get("genre"),
                    "output__activite": profil_data.get("activite"),
                    "output__allocation": profil_data.get("allocation"),
                    "output__statut_administratif": profil_data.get(
                        "statut_administratif"
                    ),
                    "output__lieu_residence": profil_data.get("lieu_residence"),
                    "output__handicap": profil_data.get("handicap"),
                    "output__famille": profil_data.get("famille"),
                    "output__adherent": profil_data.get("adherent"),
                }
            )

    profils_df = pl.DataFrame(data=profils).with_columns(
        pl.lit(pendulum.now(tz="Europe/Paris"))
        .dt.convert_time_zone("UTC")
        .alias("extracted_at"),
    )

    return validate_profils_df(profils_df)
