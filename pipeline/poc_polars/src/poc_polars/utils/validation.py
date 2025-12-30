import json
from dataclasses import dataclass
from typing import Any

import polars as pl
import pydantic

from data_inclusion.schema import v1 as schema


@dataclass
class ValidationResult:
    is_valid: bool
    errors: list[tuple[str, str, Any]]  # (field, message, value)
    row_data: dict[str, Any]


def validate_structure(row: dict[str, Any]) -> ValidationResult:
    try:
        schema.Structure(**row)
        return ValidationResult(is_valid=True, errors=[], row_data=row)
    except pydantic.ValidationError as e:
        errors = []
        for err in e.errors():
            field = str(err["loc"])
            msg = err["msg"]
            val = row
            for key in err["loc"]:
                if isinstance(val, dict):
                    val = val.get(key)
                else:
                    val = None
                    break
            errors.append((field, msg, val))
        return ValidationResult(is_valid=False, errors=errors, row_data=row)


def validate_service(row: dict[str, Any]) -> ValidationResult:
    try:
        schema.Service(**row)
        return ValidationResult(is_valid=True, errors=[], row_data=row)
    except pydantic.ValidationError as e:
        errors = []
        for err in e.errors():
            field = str(err["loc"])
            msg = err["msg"]
            val = row
            for key in err["loc"]:
                if isinstance(val, dict):
                    val = val.get(key)
                else:
                    val = None
                    break
            errors.append((field, msg, val))
        return ValidationResult(is_valid=False, errors=errors, row_data=row)


def _stringify_value(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, dict | list):
        return json.dumps(val, ensure_ascii=False, default=str)
    return str(val)


def validate_dataframe(
    df: pl.DataFrame, resource_type: str
) -> tuple[pl.DataFrame, pl.DataFrame]:
    validate_fn = (
        validate_structure if resource_type == "structure" else validate_service
    )
    rows = df.to_dicts()
    results = [validate_fn(row) for row in rows]

    valid_rows = [r.row_data for r in results if r.is_valid]

    rejected_rows = []
    for r in results:
        if not r.is_valid:
            row_data = r.row_data
            for field, msg, val in r.errors:
                rejected_rows.append(
                    {
                        "source": row_data.get("source", ""),
                        "id": row_data.get("id", ""),
                        "nom": row_data.get("nom", ""),
                        "resource_type": resource_type,
                        "field": field,
                        "reason": msg,
                        "value": _stringify_value(val)[:1000],
                    }
                )

    valid_df = (
        pl.DataFrame(valid_rows, infer_schema_length=None)
        if valid_rows
        else pl.DataFrame(schema=df.schema)
    )

    rejection_schema = {
        "source": pl.Utf8,
        "id": pl.Utf8,
        "nom": pl.Utf8,
        "resource_type": pl.Utf8,
        "field": pl.Utf8,
        "reason": pl.Utf8,
        "value": pl.Utf8,
    }
    rejected_df = (
        pl.DataFrame(rejected_rows, schema=rejection_schema)
        if rejected_rows
        else pl.DataFrame(schema=rejection_schema)
    )
    return valid_df, rejected_df
