import json
import logging
from datetime import timedelta
from pathlib import Path

import minio
import numpy as np
import pandas as pd
import pydantic
import sentry_sdk
import sqlalchemy as sqla
from furl import furl

from data_inclusion.api.config import settings
from data_inclusion.api.core import db
from data_inclusion.api.inclusion_data import models
from data_inclusion.schema import v0 as schema

logger = logging.getLogger(__name__)

sentry_sdk.init(
    dsn=settings.SENTRY_DSN,
    environment=settings.ENV,
)


class DatalakeClient:
    MARTS_PREFIX = "data/marts"

    def __init__(self) -> None:
        self.bucket_name = settings.DATALAKE_BUCKET_NAME

        endpoint_url = furl(settings.DATALAKE_ENDPOINT_URL)
        self.s3_client = minio.Minio(
            endpoint=str(endpoint_url.netloc),
            access_key=settings.DATALAKE_ACCESS_KEY,
            secret_key=settings.DATALAKE_SECRET_KEY,
            secure=endpoint_url.scheme == "https",
            region="fr-par" if "fr-par" in str(endpoint_url) else None,
        )

    def get_latest_dataset_path(self) -> Path:
        object_lists = self.s3_client.list_objects(
            bucket_name=self.bucket_name,
            prefix=self.MARTS_PREFIX,
            recursive=True,
        )

        object_lists = sorted(object_lists, key=lambda o: o.object_name)
        return Path(object_lists[-1].object_name).parent

    def get_dataset_presigned_urls(self) -> dict:
        latest_dataset_path = self.get_latest_dataset_path()

        logger.info("Using %s", latest_dataset_path)

        object_lists = self.s3_client.list_objects(
            self.bucket_name,
            prefix=str(latest_dataset_path),
            recursive=True,
        )

        presigned_urls = {}

        for object in object_lists:
            filename = Path(object.object_name).name
            presigned_url = self.s3_client.get_presigned_url(
                method="GET",
                bucket_name=self.bucket_name,
                object_name=object.object_name,
                expires=timedelta(minutes=5),
            )
            presigned_urls[filename] = presigned_url

        return presigned_urls


def fetch_dataset() -> tuple[pd.DataFrame, pd.DataFrame]:
    datalake_client = DatalakeClient()

    url_by_filename = datalake_client.get_dataset_presigned_urls()

    def df_from_url(url):
        return pd.read_parquet(url).replace({np.nan: None})

    return (
        df_from_url(url_by_filename["structures.parquet"]),
        df_from_url(url_by_filename["services.parquet"]),
    )


def validate_dataset(
    db_session,
    structures_df: pd.DataFrame,
    services_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
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
            key, value = ".".join(error["loc"]), error["input"]
            logger.warning(f"{model:20} {key=:20} {value=}")

        return len(errors) == 0

    structures_df = structures_df[
        structures_df.apply(lambda d: validate_data(schema.Structure, d), axis=1)
    ]
    services_df = services_df[
        services_df.apply(lambda d: validate_data(schema.Service, d), axis=1)
    ]

    # remove orphan services
    services_df = services_df[
        services_df._di_structure_surrogate_id.isin(structures_df._di_surrogate_id)
    ]

    return structures_df, services_df


def prepare_dataset(
    structures_df: pd.DataFrame,
    services_df: pd.DataFrame,
):
    service_scores = (
        services_df.groupby("_di_structure_surrogate_id")["score_qualite"]
        .mean()
        .round(2)
    )

    structures_df["score_qualite"] = (
        structures_df["_di_surrogate_id"].map(service_scores).astype(float).fillna(0.0)
    )

    clusters_df = structures_df[structures_df["cluster_id"].notna()]
    cluster_groups = {
        key: group.to_dict(orient="records")
        for key, group in clusters_df.groupby("cluster_id")
    }

    def get_doublons(row):
        all_ids = cluster_groups.get(row["cluster_id"], [])
        return [
            json.loads(schema.Structure(**d).model_dump_json())
            for d in all_ids
            if d["_di_surrogate_id"] != row["_di_surrogate_id"]
        ]

    structures_df["doublons"] = structures_df.apply(get_doublons, axis=1)

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

    structures_df["cluster_best_duplicate"] = (
        structures_df["cluster_id"]
        .map(cluster_master_mapping)
        .replace([np.nan], [None])
    )

    return structures_df, services_df


def load_truncate_insert(
    db_session,
    structures_df: pd.DataFrame,
    services_df: pd.DataFrame,
):
    db_session.execute(
        sqla.text(f"""
            DELETE FROM {models.Service.__tablename__};
            DELETE FROM {models.Structure.__tablename__};
        """)
    )

    for df, model in [
        (structures_df, models.Structure),
        (services_df, models.Service),
    ]:
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
def load_inclusion_data():
    logger.info("Fetching data...")
    structures_df, services_df = fetch_dataset()

    logger.info("Validating data...")
    with db.SessionLocal() as db_session:
        structures_df, services_df = validate_dataset(
            db_session, structures_df, services_df
        )

    logger.info("Preparing data...")
    structures_df, services_df = prepare_dataset(structures_df, services_df)

    logger.info("Loading data...")
    with db.SessionLocal() as db_session:
        load_truncate_insert(db_session, structures_df, services_df)

    logger.info("Vacuuming...")
    with db.default_db_engine.connect().execution_options(
        isolation_level="AUTOCOMMIT"
    ) as connection:
        connection.execute(
            sqla.text(f"VACUUM ANALYZE {models.Structure.__tablename__}")
        )
        connection.execute(sqla.text(f"VACUUM ANALYZE {models.Service.__tablename__}"))
