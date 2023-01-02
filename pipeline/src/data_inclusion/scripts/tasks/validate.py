import logging
from typing import Optional

import numpy as np
import pandas as pd
import pydantic

from data_inclusion.schema import models

logger = logging.getLogger(__name__)


def validate_structure_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    data_df = pd.json_normalize(df.data)
    data_normalized_df = pd.json_normalize(df.data_normalized)

    if "code_insee" not in data_df:
        data_df = data_df.assign(code_insee=None)

    if "siret" not in data_df:
        data_df = data_df.assign(siret=None)

    data_df = data_df.fillna(data_normalized_df)
    data_df = data_df.replace({np.nan: None})

    def validate_row(data: dict) -> Optional[list]:
        try:
            models.Structure(**data)
        except pydantic.ValidationError as exc:
            return exc.errors()

        return None

    data_df = data_df.assign(errors=lambda x: x.apply(validate_row, axis=1))

    errors_df = (
        data_df.pipe(lambda x: x[["id", "errors"]])
        .pipe(
            lambda x: pd.json_normalize(
                x.to_dict(orient="records"), record_path="errors", meta="id"
            )
        )
        .pipe(lambda x: x.drop(columns=x.columns[x.columns.str.startswith("ctx.")]))
    )

    data_df = data_df.assign(is_valid=lambda x: x.errors.isna()).drop(columns="errors")

    logger.info("Résultats de la validation:")
    logger.info(f"\t{len(errors_df)} erreurs détectées")
    logger.info(f"\t{(~data_df.is_valid).sum()} lignes non conformes")
    logger.info(f"\t{data_df.is_valid.sum()} lignes conformes")
    if len(errors_df) > 0:
        logger.info(f"\t{errors_df[['loc', 'msg', 'type']].value_counts()}")

    return data_df


def validate_service_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    data_df = pd.json_normalize(df.data)
    data_df = data_df.replace({np.nan: None})

    def validate_row(data: dict) -> Optional[list]:
        try:
            models.Service(**data)
        except pydantic.ValidationError as exc:
            return exc.errors()

        return None

    data_df = data_df.assign(errors=lambda x: x.apply(validate_row, axis=1))

    errors_df = (
        data_df.pipe(lambda x: x[["id", "errors"]])
        .pipe(
            lambda x: pd.json_normalize(
                x.to_dict(orient="records"), record_path="errors", meta="id"
            )
        )
        .pipe(lambda x: x.drop(columns=x.columns[x.columns.str.startswith("ctx.")]))
    )

    data_df = data_df.assign(is_valid=lambda x: x.errors.isna()).drop(columns="errors")

    logger.info("Résultats de la validation:")
    logger.info(f"\t{len(errors_df)} erreurs détectées")
    logger.info(f"\t{(~data_df.is_valid).sum()} lignes non conformes")
    logger.info(f"\t{data_df.is_valid.sum()} lignes conformes")
    if len(errors_df) > 0:
        logger.info(f"\t{errors_df[['loc', 'msg', 'type']].value_counts()}")

    return data_df
