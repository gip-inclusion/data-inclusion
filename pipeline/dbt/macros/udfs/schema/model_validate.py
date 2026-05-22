import json
from typing import Literal, TypedDict

import pydantic

from data_inclusion import schema


class ValidationErrorDict(TypedDict):
    model_class: str
    type: str
    loc: str
    msg: str
    input: str


def model_validate(
    model: Literal[
        "data_inclusion.schema.v1.Structure",
        "data_inclusion.schema.v1.Service",
    ],
    data: str,
) -> list[ValidationErrorDict]:
    """Validate the input data against the Pydantic model.

    Args:
        model_class (str): The fully qualified name of the Pydantic model class.
        data (str): The input data to validate, as a JSON string.

    Returns:
        ValidationErrorDict: A dictionary containing validation error details,
        or an empty dictionary if validation is successful.
    """

    pydantic_model = {
        "data_inclusion.schema.v1.Structure": schema.v1.Structure,
        "data_inclusion.schema.v1.Service": schema.v1.Service,
    }[model]

    try:
        pydantic_model.model_validate_json(data)
    except pydantic.ValidationError as exc:
        return [
            {
                "model_class": model,
                "type": error["type"],
                "loc": ".".join(map(str, error["loc"])),
                "msg": error["msg"],
                "input": json.dumps(error["input"]),
            }
            for error in exc.errors(include_url=False)
        ]

    return []
