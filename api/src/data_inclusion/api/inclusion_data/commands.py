import logging
from datetime import timedelta
from pathlib import Path

import minio
import numpy as np
import pandas as pd
import pydantic
import sqlalchemy as sqla
from furl import furl
from tqdm import tqdm

from data_inclusion import schema
from data_inclusion.api.code_officiel_geo import constants
from data_inclusion.api.config import settings
from data_inclusion.api.core import db
from data_inclusion.api.inclusion_data import models

logger = logging.getLogger(__name__)


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

        logger.info(f"Using {latest_dataset_path}")

        object_lists = self.s3_client.list_objects(
            self.bucket_name,
            prefix=str(latest_dataset_path),
            recursive=True,
        )

        presigned_urls = {}

        for object in object_lists:
            ressource = Path(object.object_name).name.split(".")[0]
            presigned_url = self.s3_client.get_presigned_url(
                method="GET",
                bucket_name=self.bucket_name,
                object_name=object.object_name,
                expires=timedelta(minutes=5),
            )
            presigned_urls[ressource] = presigned_url

        return presigned_urls


def load_di_dataset_as_dataframes() -> dict[str, pd.DataFrame]:
    datalake_client = DatalakeClient()

    url_by_ressource = datalake_client.get_dataset_presigned_urls()

    return {
        ressource: pd.read_parquet(url) for ressource, url in url_by_ressource.items()
    }


def validate_df(df: pd.DataFrame, model_schema) -> pd.DataFrame:
    """Apply pydantic model validation to a dataframe

    Returns a validation errors as a dataframe
    """
    errors_sr = df.apply(lambda d: validate_data(model_schema, d), axis="columns")
    df = df.assign(errors=errors_sr)
    errors_df = df[["_di_surrogate_id", "source", "errors"]]
    errors_df = errors_df.dropna(subset="errors").explode("errors")
    errors_df = pd.json_normalize(errors_df.to_dict(orient="records"))
    errors_df = errors_df.assign(model=model_schema.__name__)
    return errors_df


def log_errors(errors_df: pd.DataFrame):
    if errors_df.empty:
        logger.info("no error")
        return
    info_str = str(
        errors_df.groupby(["source", "errors.loc"])["_di_surrogate_id"]
        .count()
        .unstack()
    )
    logger.info("\n" + info_str, stacklevel=2)


def load_inclusion_data():
    """Download, validate and load the di dataset

    1. Identify the latest version in the datalake
    2. Generate presigned URLs for it
    3. Download into dataframes
    4. Validate data against pydantic models
    5. Log validation errors
    6. Load valid data using the sqla models
    """
    df_by_ressource = load_di_dataset_as_dataframes()

    structures_df = df_by_ressource["structures"]
    services_df = df_by_ressource["services"]

    structures_df = structures_df.replace({np.nan: None})
    services_df = services_df.replace({np.nan: None})

    # TODO: this must be fixed in the publication
    structures_df = structures_df.assign(
        code_insee=structures_df.code_insee.apply(clean_up_code_insee)
    )
    services_df = services_df.assign(
        code_insee=services_df.code_insee.apply(clean_up_code_insee)
    )

    # fill missing codes with geocoding results
    # and overwrite existing ones if the geocoder is confident enough
    geocoder_validity_threshold = 0.7
    structures_df = structures_df.assign(
        code_insee=structures_df.code_insee.where(
            structures_df._di_geocodage_score > geocoder_validity_threshold,
            structures_df._di_geocodage_code_insee,
        ).fillna(structures_df._di_geocodage_code_insee)
    )
    services_df = services_df.assign(
        code_insee=services_df.code_insee.where(
            services_df._di_geocodage_score > geocoder_validity_threshold,
            services_df._di_geocodage_code_insee,
        ).fillna(structures_df._di_geocodage_code_insee)
    )
    structures_df = structures_df.drop(
        columns=["_di_geocodage_code_insee", "_di_geocodage_score"]
    )
    services_df = services_df.drop(
        columns=["_di_geocodage_code_insee", "_di_geocodage_score"]
    )

    structure_errors_df = validate_df(structures_df, model_schema=schema.Structure)
    service_errors_df = validate_df(services_df, model_schema=schema.Service)

    logger.info("Structure validation errors:")
    log_errors(structure_errors_df)
    logger.info("Services validation errors:")
    log_errors(service_errors_df)

    # exclude invalid data
    if not structure_errors_df.empty:
        structures_df = structures_df[
            ~structures_df._di_surrogate_id.isin(structure_errors_df._di_surrogate_id)
        ]
    if not service_errors_df.empty:
        services_df = services_df[
            ~services_df._di_surrogate_id.isin(service_errors_df._di_surrogate_id)
            & ~services_df._di_structure_surrogate_id.isin(
                structure_errors_df._di_surrogate_id
            )
        ]

    structure_data_list = structures_df.to_dict(orient="records")
    service_data_list = services_df.to_dict(orient="records")

    # TODO(vmttn): load in a temporary table, truncate and then insert
    with db.SessionLocal() as session:
        session.execute(sqla.delete(models.Service))
        session.execute(sqla.delete(models.Structure))

        for structure_data in tqdm(structure_data_list):
            structure_instance = models.Structure(**structure_data)
            try:
                with session.begin_nested():
                    session.add(structure_instance)
            except sqla.exc.IntegrityError as exc:
                logger.error(
                    f"Structure source={structure_data['source']} "
                    f"id={structure_data['id']}"
                )
                logger.info(exc.orig)

        for service_data in tqdm(service_data_list):
            service_instance = models.Service(**service_data)
            try:
                with session.begin_nested():
                    session.add(service_instance)
            except sqla.exc.IntegrityError as exc:
                logger.error(
                    f"Service source={service_data['source']} id={service_data['id']}"
                )
                logger.info(exc.orig)

        session.commit()

    with db.default_db_engine.connect().execution_options(
        isolation_level="AUTOCOMMIT"
    ) as connection:
        connection.execute(sqla.text("VACUUM ANALYZE api__structures"))
        connection.execute(sqla.text("VACUUM ANALYZE api__services"))


def clean_up_code_insee(v) -> str | None:
    return constants.CODE_COMMUNE_BY_CODE_ARRONDISSEMENT.get(v, v)


def validate_data(model_schema, data):
    try:
        model_schema(**data)
    except pydantic.ValidationError as exc:
        return exc.errors()
