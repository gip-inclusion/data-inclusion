import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pydantic
import sentry_sdk
import sqlalchemy as sqla
from sqlalchemy import orm

from data_inclusion.api.config import settings
from data_inclusion.api.decoupage_administratif.models import Commune
from data_inclusion.api.inclusion_data.v0 import models as v0_models
from data_inclusion.api.inclusion_data.v1 import models as v1_models
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
    structure_schema: pydantic.BaseModel,
    service_schema: pydantic.BaseModel,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run validation on the dataset."""
    city_codes = db_session.scalars(sqla.select(Commune.code)).all()

    def validate_data(model_schema, data):
        errors = []

        try:
            model_schema(**data)
        except pydantic.ValidationError as exc:
            errors += [{"id": data["_di_surrogate_id"], **err} for err in exc.errors()]

        if data["code_insee"] is not None and data["code_insee"] not in city_codes:
            errors += [
                {
                    "id": data["_di_surrogate_id"],
                    "loc": ("code_insee",),
                    "input": data["code_insee"],
                }
            ]

        for error in errors:
            model = model_schema.__name__
            id, key, value = (
                error["id"][:50],
                ".".join(map(str, error["loc"])),
                error["input"],
            )
            logger.warning(f"{model:10} {id=:52} {key=:20} {value=}")

        return errors

    def is_valid(df, model_schema):
        return df.apply(lambda d: len(validate_data(model_schema, d)) == 0, axis=1)

    if not structures_df.empty:
        structures_df = structures_df.loc[is_valid(structures_df, structure_schema)]
        structures_df = structures_df.loc[
            structures_df["code_insee"].apply(lambda c: c is None or c in city_codes)
        ]

    if not services_df.empty:
        services_df = services_df.loc[is_valid(services_df, service_schema)]
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
    db_session,
    df: pd.DataFrame,
    model,
):
    """Replace a table with a DataFrame.

    Does not commit the transaction, caller must do it.
    """
    db_session.execute(sqla.delete(model))

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
    structure_model,
    service_model,
):
    load_df_to_table(db_session, structures_df, structure_model)
    load_df_to_table(db_session, services_df, service_model)

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
    for (
        schema_version,
        structure_model,
        service_model,
        structure_schema,
        service_schema,
    ) in [
        ("v0", v0_models.Structure, v0_models.Service, v0.Structure, v0.Service),
        ("v1", v1_models.Structure, v1_models.Service, v1.Structure, v1.Service),
    ]:
        structures_df, services_df = [
            pd.read_parquet(path / filename).replace({np.nan: None})
            for filename in ("structures.parquet", "services.parquet")
        ]

        if schema_version == "v1":
            services_df = services_df.drop(columns="frais", errors="ignore")
            services_df = services_df.rename(columns={"frais_v1": "frais"})

        logger.info(f"{schema_version=} Validating data...")
        structures_df, services_df = validate_dataset(
            db_session=db_session,
            structures_df=structures_df,
            services_df=services_df,
            structure_schema=structure_schema,
            service_schema=service_schema,
        )

        services_df = services_df.drop(columns="score_qualite", errors="ignore")
        services_df = services_df.rename(
            columns={f"score_qualite_{schema_version}": "score_qualite"}
        )
        services_df["score_qualite"] = pd.to_numeric(
            services_df["score_qualite"], errors="raise"
        )

        logger.info(f"{schema_version=} Preparing data...")
        structures_df, services_df = prepare_dataset(
            structures_df=structures_df,
            services_df=services_df,
        )

        logger.info(f"{schema_version=} Loading data...")
        load_dataset(
            db_session=db_session,
            structures_df=structures_df,
            services_df=services_df,
            structure_model=structure_model,
            service_model=service_model,
        )

        logger.info("Vacuuming...")
        with (
            db_session.get_bind()
            .engine.connect()
            .execution_options(isolation_level="AUTOCOMMIT") as connection
        ):
            for model in (structure_model, service_model):
                connection.execute(sqla.text(f"VACUUM ANALYZE {model.__tablename__}"))
