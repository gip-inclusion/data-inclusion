from typing import TypedDict

import pydantic
import pydantic_core
from sqlalchemy import orm

from data_inclusion.api.valideur import schemas, translations, utils
from data_inclusion.schema import v1


class ValidationError(TypedDict):
    schema: str
    field: str
    line: int
    value: object | None
    message: str


def validate_dataset(
    structures_data: list[dict],
    services_data: list[dict],
    db_session: orm.Session,
    structure_model: type[v1.Structure] = schemas.ValideurInputStructure,
    service_model: type[v1.Service] = schemas.ValideurInputService,
) -> list[ValidationError]:
    return [
        *list_errors(structure_model, structures_data, db_session),
        *list_errors(service_model, services_data, db_session),
    ]


def _format_error(
    model: type[v1.Structure] | type[v1.Service],
    line: int,
    error_details: pydantic_core.ErrorDetails,
) -> ValidationError:
    field, *_ = error_details["loc"]

    if error_details["type"] == "enum":
        LOC_TO_ENUM = {
            "thematiques": v1.Thematique,
            "publics": v1.Public,
        }
        if str(field) in LOC_TO_ENUM:
            error_details = utils.enum_error_message_with_suggestion(
                error_details=error_details,
                enum_cls=LOC_TO_ENUM[str(field)],
            )

    if issubclass(model, v1.Structure):
        schema_name = "Structure"
    elif issubclass(model, v1.Service):
        schema_name = "Service"
    else:
        raise RuntimeError(f"Unknown model type: {model}")

    return {
        "schema": schema_name,
        "field": str(field),
        "line": line,
        "value": error_details["input"] if error_details["type"] != "missing" else None,
        "message": translations.tr(error_details["msg"]),
    }


def _list_errors(
    model: type[v1.Structure] | type[v1.Service],
    data: dict,
    db_session: orm.Session | None,
) -> list[pydantic_core.ErrorDetails]:
    try:
        model.model_validate(
            data,
            context={
                "db_session": db_session,
                "ignore_warnings": False,
            },
        )
    except pydantic.ValidationError as exc:
        return exc.errors()

    return []


def list_errors(
    model: type[v1.Structure] | type[v1.Service],
    data: list[dict],
    db_session: orm.Session | None = None,
) -> list[ValidationError]:
    use_line_numbers = all("line" in d for d in data)

    return [
        _format_error(
            model=model,
            line=record["line"] if use_line_numbers else index,
            error_details=error_details,
        )
        for index, record in enumerate(data)
        for error_details in _list_errors(model, record, db_session)
    ]
