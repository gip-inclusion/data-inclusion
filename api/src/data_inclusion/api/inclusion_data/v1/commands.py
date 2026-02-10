import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pydantic
import sqlalchemy as sqla
from sqlalchemy import orm

from data_inclusion.api.decoupage_administratif.models import Commune
from data_inclusion.api.inclusion_data.v1 import models
from data_inclusion.schema import v1

logger = logging.getLogger(__name__)


def validate_data(model_schema, data: dict) -> list[dict]:
    errors = []

    try:
        model_schema(**data)
    except pydantic.ValidationError as exc:
        errors += [{"id": data["id"], **err} for err in exc.errors()]

    return errors


def prepare_load(
    db_session: orm.Session,
    structures_df: pd.DataFrame,
    services_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Prepare the structures and services dataframes for loading into the database.

    Remove invalid entries, compute quality scores, etc.
    """
    if structures_df.empty or services_df.empty:
        raise ValueError("Structures or services dataset is empty.")

    city_codes = db_session.scalars(sqla.select(Commune.code)).all()

    structures_errors_df = structures_df.apply(
        lambda d: validate_data(v1.Structure, d), axis=1
    )
    structures_df = structures_df.loc[structures_errors_df.apply(len) == 0]
    valid_code_insee_idx = structures_df["code_insee"].apply(
        lambda c: c is None or c in city_codes
    )
    structures_df = structures_df.loc[valid_code_insee_idx]
    structures_df = structures_df.loc[~structures_df["_is_closed"]]
    structures_df = structures_df.loc[structures_df["_has_valid_address"].fillna(False)]

    services_errors_df = services_df.apply(
        lambda d: validate_data(v1.Service, d), axis=1
    )
    services_df = services_df.loc[services_errors_df.apply(len) == 0]
    valid_code_insee_idx = services_df["code_insee"].apply(
        lambda c: c is None or c in city_codes
    )
    services_df = services_df.loc[valid_code_insee_idx]
    services_df = services_df.loc[services_df["_has_valid_address"].fillna(False)]
    services_df = services_df.loc[services_df["structure_id"].isin(structures_df["id"])]

    service_scores = (
        services_df.groupby("structure_id")["score_qualite"]
        .mean()
        .astype(float)
        .round(2)
    )

    structures_df = structures_df.assign(
        score_qualite=structures_df["id"].map(service_scores).astype(float).fillna(0.0)
    )

    if "_extra" in services_df.columns:
        services_df = services_df.assign(
            extra=services_df["_extra"].apply(
                lambda x: json.loads(x) if x is not None else None
            )
        )

    return structures_df, services_df


setup_sql = """
    -- prevent this transaction from lasting too long
    -- it will fail rather than blocking other transactions
    SET LOCAL lock_timeout = '5s';

    DROP SCHEMA IF EXISTS load_tmp CASCADE;
    DROP SCHEMA IF EXISTS load_old CASCADE;

    CREATE SCHEMA load_tmp;
    CREATE SCHEMA load_old;

    CREATE TABLE load_tmp.api__structures_v1 (LIKE public.api__structures_v1 INCLUDING ALL);
    CREATE TABLE load_tmp.api__services_v1   (LIKE public.api__services_v1   INCLUDING ALL);
"""  # noqa: E501

swap_sql = """
    -- prevent this transaction from lasting too long
    -- it will fail rather than blocking other transactions
    SET LOCAL lock_timeout = '5s';

    -- drop foreign keys that point to one of the old tables
    ALTER TABLE public.api__services_v1
        DROP CONSTRAINT fk_api__services_v1__structure_id__api__structures_v1;

    -- swap both structures and services tables
    -- using SET SCHEMA over RENAME in order to keep indexes names consistent
    -- with the sqlalchemy models managed by alembic
    ALTER TABLE public.api__structures_v1      SET SCHEMA load_old;
    ALTER TABLE public.api__services_v1        SET SCHEMA load_old;
    ALTER TABLE load_tmp.api__structures_v1    SET SCHEMA public;
    ALTER TABLE load_tmp.api__services_v1      SET SCHEMA public;

    -- recreate foreign keys that point to the new tables
    ALTER TABLE public.api__services_v1
        ADD CONSTRAINT fk_api__services_v1__structure_id__api__structures_v1
        FOREIGN KEY (structure_id)
        REFERENCES public.api__structures_v1 (id)
        ON DELETE CASCADE;
"""


def apply_load(
    engine: sqla.Engine,  # manage transactions manually
    structures_df: pd.DataFrame,
    services_df: pd.DataFrame,
):
    logger.info("Setting up staging tables...")
    with engine.connect() as conn:
        conn.execute(sqla.text(setup_sql))
        conn.commit()

    logger.info("Loading data into staging tables...")
    with engine.connect() as conn:
        for model, df in [
            (models.Structure, structures_df),
            (models.Service, services_df),
        ]:
            # compute the list of columns to insert
            # ignoring server computed columns
            columns_list = [
                c
                for c in model.__table__.columns
                if c.server_default is None
                and c.name not in ["cluster_best_duplicate", "doublons"]
            ]

            df = df.sort_values(by="id", ascending=True)
            df = df[[c.name for c in columns_list]]

            df.to_sql(
                name=model.__tablename__,
                schema="load_tmp",
                con=conn,
                if_exists="append",
                index=False,
                dtype={column.name: column.type for column in columns_list},
            )
        conn.commit()

    # Analyze before swapping to have accurate statistics right away
    logger.info("Analyzing staging tables...")
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(sqla.text("ANALYZE load_tmp.api__structures_v1"))
        conn.execute(sqla.text("ANALYZE load_tmp.api__services_v1"))

    # The following requires an exclusive lock (because of the swap).
    # Therefore it SHOULD stay minimal. Any operations that can be
    # done without an exclusive lock should be done in other transactions
    # to prevent locking everything for too long.
    logger.info("Swapping staging tables with production tables...")
    with engine.connect() as conn:
        conn.execute(sqla.text(swap_sql))
        conn.commit()

    with engine.connect() as conn:
        structures_count = conn.execute(
            sqla.select(sqla.func.count()).select_from(models.Structure)
        )
        services_count = conn.execute(
            sqla.select(sqla.func.count()).select_from(models.Service)
        )
        logger.info(f"{structures_count.scalar()} structures loaded.")
        logger.info(f"{services_count.scalar()} services loaded.")


def load(db_session: orm.Session, path: Path):
    structures_df, services_df = [
        pd.read_parquet(path / filename).replace({np.nan: None})
        for filename in ("structures.parquet", "services.parquet")
    ]

    structures_df, services_df = prepare_load(
        db_session=db_session,
        structures_df=structures_df,
        services_df=services_df,
    )

    apply_load(
        engine=db_session.get_bind().engine,
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
