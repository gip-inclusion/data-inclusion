import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pydantic
import sqlalchemy as sqla
from sqlalchemy import orm

from data_inclusion.api.decoupage_administratif.models import Commune
from data_inclusion.api.inclusion_data.v0 import models
from data_inclusion.schema import v0

logger = logging.getLogger(__name__)


def validate_dataset(
    db_session: orm.Session,
    structures_df: pd.DataFrame,
    services_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    city_codes = db_session.scalars(sqla.select(Commune.code)).all()

    def validate_data(model_schema, data):
        errors = []

        try:
            model_schema(**data)
        except pydantic.ValidationError as exc:
            errors += [
                {"_di_surrogate_id": data["_di_surrogate_id"], **err}
                for err in exc.errors()
            ]

        if data["code_insee"] is not None and data["code_insee"] not in city_codes:
            errors += [
                {
                    "_di_surrogate_id": data["_di_surrogate_id"],
                    "loc": ("code_insee",),
                    "input": data["code_insee"],
                }
            ]

        for error in errors:
            model = model_schema.__name__
            _di_surrogate_id, key, value = (
                error["_di_surrogate_id"][:50],
                ".".join(map(str, error["loc"])),
                error["input"],
            )
            logger.warning(f"{model:10} {_di_surrogate_id=:52} {key=:20} {value=}")

        return errors

    def is_valid(df, model_schema):
        return df.apply(lambda d: len(validate_data(model_schema, d)) == 0, axis=1)

    if not structures_df.empty:
        structures_df = structures_df.loc[is_valid(structures_df, v0.Structure)]
        structures_df = structures_df.loc[
            structures_df["code_insee"].apply(lambda c: c is None or c in city_codes)
        ]
        structures_df = structures_df.loc[~structures_df["_is_closed"]]

    if not services_df.empty:
        services_df = services_df.loc[is_valid(services_df, v0.Service)]
        services_df = services_df.loc[
            services_df["code_insee"].apply(lambda c: c is None or c in city_codes)
        ]
        services_df = services_df.loc[
            services_df["_di_structure_surrogate_id"].isin(
                structures_df["_di_surrogate_id"]
            )
        ]

    return structures_df, services_df


def prepare_dataset(
    structures_df: pd.DataFrame,
    services_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    service_scores = (
        services_df.groupby("_di_structure_surrogate_id")["score_qualite"]
        .mean()
        .astype(float)
        .round(2)
    )

    structures_df = structures_df.assign(
        score_qualite=structures_df["_di_surrogate_id"]
        .map(service_scores)
        .astype(float)
        .fillna(0.0)
    )

    structures_df = structures_df.assign(
        _is_best_duplicate=pd.Series(
            structures_df.index.isin(
                structures_df.sort_values("date_maj", ascending=False)
                .sort_values("score_qualite")
                .groupby("_cluster_id")["score_qualite"]
                .idxmax()
            )
        ).where(structures_df["_cluster_id"].notna(), other=None)
    )

    return structures_df, services_df


def load_df_to_table(
    db_session: orm.Session,
    df: pd.DataFrame,
    model,
):
    """Replace a table with a DataFrame.

    Does not commit the transaction, caller must do it.
    """
    columns_list = [
        c
        for c in sorted(model.__table__.columns, key=lambda c: c.name)
        # ignore server computed columns
        if c.server_default is None
        and c.name not in ["cluster_best_duplicate", "doublons"]
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
    db_session.execute(sqla.delete(models.Service))
    db_session.execute(sqla.delete(models.Structure))
    load_df_to_table(db_session, structures_df, models.Structure)
    load_df_to_table(db_session, services_df, models.Service)

    db_session.commit()


def load_inclusion_data(db_session: orm.Session, path: Path):
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
        for model in [models.Structure, models.Service]:
            connection.execute(sqla.text(f"VACUUM ANALYZE {model.__tablename__}"))
