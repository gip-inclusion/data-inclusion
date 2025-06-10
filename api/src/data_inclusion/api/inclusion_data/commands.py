import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pydantic
import sentry_sdk
import sqlalchemy as sqla
from sqlalchemy import orm

from data_inclusion.api.config import settings
from data_inclusion.api.inclusion_data import models
from data_inclusion.schema import v0, v1

logger = logging.getLogger(__name__)

sentry_sdk.init(
    dsn=settings.SENTRY_DSN,
    environment=settings.ENV,
)


def validate_dataset(
    db_session,
    structures_df: pd.DataFrame,
    services_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run validation on the dataset.

    Validate each dataframe against both versions of the schema (v0 and v1).
    Add `_is_valid_v0` and `_is_valid_v1` columns to the dataframes.
    Remove invalid `code_insee` values that are not present in the database.
    """
    city_codes = db_session.scalars(sqla.select(models.Commune.code)).all()

    def validate_data(model_schema, data):
        errors = []

        try:
            model_schema(**data)
        except pydantic.ValidationError as exc:
            errors += exc.errors()

        if data["code_insee"] is not None and data["code_insee"] not in city_codes:
            errors += [{"loc": ("code_insee",), "input": data["code_insee"]}]

        for error in errors:
            model = model_schema.__name__
            key, value = ".".join(map(str, error["loc"])), error["input"]
            logger.warning(f"{model:20} {key=:20} {value=}")

        return errors

    def remove_invalid_code_insee(df):
        return df.assign(
            code_insee=df["code_insee"].mask(
                cond=df["code_insee"].apply(
                    lambda c: c is not None or c not in city_codes
                ),
                other=None,
            )
        )

    def is_valid(df, model_schema):
        return df.apply(lambda d: len(validate_data(model_schema, d)) == 0, axis=1)

    if not structures_df.empty:
        structures_df = structures_df.assign(
            _is_valid_v0=is_valid(structures_df, v0.Structure)
        )
        structures_df = structures_df.assign(
            _is_valid_v1=is_valid(structures_df, v1.Structure)
        )
        structures_df = remove_invalid_code_insee(structures_df)

    if not services_df.empty:
        services_df = services_df.assign(_is_valid_v0=is_valid(services_df, v0.Service))
        services_df = services_df.assign(_is_valid_v1=is_valid(services_df, v1.Service))
        services_df = remove_invalid_code_insee(services_df)

    return structures_df, services_df


def prepare_dataset(
    structures_df: pd.DataFrame,
    services_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    service_scores = (
        services_df.groupby("_di_structure_surrogate_id")["score_qualite"]
        .mean()
        .round(2)
    )

    structures_df = structures_df.assign(
        score_qualite=structures_df["_di_surrogate_id"]
        .map(service_scores)
        .astype(float)
        .fillna(0.0)
    )

    clusters_df = structures_df[structures_df["cluster_id"].notna()]
    cluster_groups = {
        key: group.to_dict(orient="records")
        for key, group in clusters_df.groupby("cluster_id")
    }

    def get_doublons(row):
        all_ids = cluster_groups.get(row["cluster_id"], [])
        return [
            json.loads(v0.Structure(**d).model_dump_json())
            for d in all_ids
            if d["_di_surrogate_id"] != row["_di_surrogate_id"]
        ]

    structures_df = structures_df.assign(
        doublons=structures_df.apply(get_doublons, axis=1)
    )

    clusters_df = (
        structures_df[structures_df["cluster_id"].notna()]
        .sort_values(
            ["cluster_id", "score_qualite", "date_maj"],
            ascending=[True, False, False],
        )
        .groupby("cluster_id")
        .first()
        .reset_index()
    )

    cluster_master_mapping = dict(
        zip(
            clusters_df["cluster_id"],
            clusters_df["_di_surrogate_id"],
        )
    )

    structures_df = structures_df.assign(
        cluster_best_duplicate=structures_df["cluster_id"]
        .map(cluster_master_mapping)
        .replace([np.nan], [None])
    )

    return structures_df, services_df


def load_df_to_table(
    db_session,
    df: pd.DataFrame,
    model,
):
    """Replace a table with a DataFrame.

    Does not commit the transaction, caller must do it.
    """
    db_session.execute(sqla.text(f"DELETE FROM {model.__tablename__}"))

    columns_list = [
        c
        for c in sorted(model.__table__.columns, key=lambda c: c.name)
        # ignore server computed columns
        if c.server_default is None
    ]

    df = df.sort_values(by="_di_surrogate_id", ascending=True)
    df = df[[c.name for c in columns_list]]

    df.to_sql(
        name=model.__tablename__,
        con=db_session.connection(),
        if_exists="append",
        index=False,
        dtype={column.name: column.type for column in columns_list},
    )


def load_dataset(
    db_session: orm.Session,
    structures_df: pd.DataFrame,
    services_df: pd.DataFrame,
):
    load_df_to_table(db_session, structures_df, models.Structure)
    load_df_to_table(db_session, services_df, models.Service)
    db_session.commit()


@sentry_sdk.monitor(
    monitor_slug="load-inclusion-data",
    monitor_config={
        "schedule": {"type": "crontab", "value": "0 * * * *"},
        "checkin_margin": 60,
        "max_runtime": 60,
        "failure_issue_threshold": 1,
        "recovery_threshold": 1,
        "timezone": "UTC",
    },
)
def load_inclusion_data(
    db_session,
    path: Path,
):
    structures_df, services_df = [
        pd.read_parquet(path / filename).replace({np.nan: None})
        for filename in ("structures.parquet", "services.parquet")
    ]

    logger.info("Validating data...")
    structures_df, services_df = validate_dataset(
        db_session=db_session,
        structures_df=structures_df,
        services_df=services_df,
    )

    logger.info("Preparing data...")
    structures_df, services_df = prepare_dataset(
        structures_df=structures_df,
        services_df=services_df,
    )

    logger.info("Loading data...")
    load_dataset(
        db_session=db_session,
        structures_df=structures_df,
        services_df=services_df,
    )

    logger.info("Vacuuming...")
    with (
        db_session.get_bind()
        .engine.connect()
        .execution_options(isolation_level="AUTOCOMMIT") as connection
    ):
        for model in (models.Structure, models.Service):
            connection.execute(sqla.text(f"VACUUM ANALYZE {model.__tablename__}"))
