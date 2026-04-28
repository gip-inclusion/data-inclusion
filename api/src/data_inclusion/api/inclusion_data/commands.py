import json
import logging
from pathlib import Path

import numpy as np
import polars as pl
import sqlalchemy as sqla
from sqlalchemy import orm

from data_inclusion.api.decoupage_administratif.models import Commune
from data_inclusion.api.inclusion_data import models
from data_inclusion.api.inclusion_data.services import build_search_index
from data_inclusion.api.valideur import services
from data_inclusion.schema import v1

logger = logging.getLogger(__name__)


def prepare_load(
    cities_df: pl.DataFrame,
    structures_df: pl.DataFrame,
    services_df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if structures_df.is_empty():
        raise ValueError("Structures list is empty.")

    if services_df.is_empty():
        raise ValueError("Services list is empty.")

    # remove closed structures according to sirene
    structures_df = structures_df.filter(~pl.col("_is_closed"))
    # remove structures with invalid city code
    structures_df = structures_df.join(
        other=structures_df.join(
            other=cities_df,
            left_on="code_insee",
            right_on="code",
            how="anti",
        ),
        on="id",
        how="anti",
    )
    # remove structures with validation errors
    structures_df = structures_df.join(
        other=pl.DataFrame(
            data=(
                row_dict["id"]
                for row_dict in structures_df.iter_rows(named=True)
                for _ in services.list_errors(model=v1.Structure, data=[{**row_dict}])
            ),
            schema={"structure_id": pl.String},
        ),
        left_on="id",
        right_on="structure_id",
        how="anti",
    )
    # exclude orphaned services
    services_df = services_df.join(
        other=structures_df,
        left_on="structure_id",
        right_on="id",
        how="semi",
    )
    # remove services with invalid city code
    services_df = services_df.join(
        other=services_df.join(
            other=cities_df,
            left_on="code_insee",
            right_on="code",
            how="anti",
        ),
        on="id",
        how="anti",
    )
    # remove services with validation errors
    services_df = services_df.join(
        other=pl.from_dicts(
            data=(
                row_dict["id"]
                for row_dict in services_df.iter_rows(named=True)
                for _ in services.list_errors(model=v1.Service, data=[{**row_dict}])
            ),
            schema={"service_id": pl.String},
        ),
        left_on="id",
        right_on="service_id",
        how="anti",
    )
    # compute quality score for structures
    # as the average of their services quality scores
    structures_df = structures_df.join(
        other=structures_df.join(
            other=services_df.select(["structure_id", "score_qualite"]),
            left_on="id",
            right_on="structure_id",
            how="left",
        )
        .group_by("id")
        .agg(
            score_qualite=pl.col("score_qualite").mean().round(2).fill_null(0.0),
        ),
        on="id",
        how="left",
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
    engine: sqla.Engine,
    structures_df: pl.DataFrame,
    services_df: pl.DataFrame,
):
    with engine.connect() as conn:
        logger.info("Setting up staging tables...")
        conn.execute(sqla.text(setup_sql))
        conn.commit()

        logger.info("Loading data into staging tables...")
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
                and c.name
                not in ["cluster_best_duplicate", "doublons", "search_vector"]
            ]

            df = df.to_pandas().replace({np.nan: None})

            # decode arbitrary json data with pandas
            # polars doesn't support this
            if "_extra" in df.columns:
                df = df.assign(
                    extra=df["_extra"].apply(
                        lambda x: json.loads(x) if x is not None else None
                    )
                )

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
        conn.execute(sqla.text("ANALYZE load_tmp.api__structures_v1"))
        conn.execute(sqla.text("ANALYZE load_tmp.api__services_v1"))
        conn.commit()

        # The following requires an exclusive lock (because of the swap).
        # Keep it minimal and commit right after !
        logger.info("Swapping staging tables with production tables...")
        conn.execute(sqla.text(swap_sql))
        conn.commit()

        structures_count = conn.execute(
            sqla.select(sqla.func.count()).select_from(models.Structure)
        )
        services_count = conn.execute(
            sqla.select(sqla.func.count()).select_from(models.Service)
        )
        logger.info(f"{structures_count.scalar()} structures loaded.")
        logger.info(f"{services_count.scalar()} services loaded.")

    logger.info("Vacuuming...")
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        for model in [models.Structure, models.Service]:
            connection.execute(sqla.text(f"VACUUM ANALYZE {model.__tablename__}"))


def load(db_session: orm.Session, path: Path):
    # use engine for fine-grained control over transactions
    engine = db_session.get_bind().engine

    structures_df = pl.read_parquet(path / "structures.parquet")
    services_df = pl.read_parquet(path / "services.parquet")

    with engine.connect() as conn:
        cities_df = pl.read_database(sqla.select(Commune.code), connection=conn)

    structures_df, services_df = prepare_load(
        cities_df=cities_df,
        structures_df=structures_df,
        services_df=services_df,
    )

    apply_load(
        engine=engine,
        structures_df=structures_df,
        services_df=services_df,
    )

    build_search_index(db_session=db_session)
