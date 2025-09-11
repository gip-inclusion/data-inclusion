{% macro udf__model_validate() %}

DROP FUNCTION IF EXISTS processings.model_validate;

CREATE OR REPLACE FUNCTION processings.model_validate(model_class TEXT, data JSONB)
RETURNS
    TABLE(
        model_class TEXT,
        type TEXT,
        loc TEXT,
        msg TEXT,
        input JSONB
    )
AS $$

import importlib
import json
from typing import TypedDict

import pydantic

class ValidationErrorDict(TypedDict):
    model_class: str
    type: str
    loc: str
    msg: str
    input: str

def model_validate(model_class: str, data: str) -> ValidationErrorDict:
    """Validate the input data against the Pydantic model.

    Args:
        model_class (str): The fully qualified name of the Pydantic model class.
        data (str): The input data to validate, as a JSON string.

    Returns:
        ValidationErrorDict: A dictionary containing validation error details,
        or an empty dictionary if validation is successful.
    """

    module_path, class_name = model_class.rsplit('.', 1)
    module = importlib.import_module(module_path)
    model = getattr(module, class_name)

    try:
        model.model_validate_json(data)
    except pydantic.ValidationError as exc:
        return [
            {
                "model_class": model_class,
                "type": error["type"],
                "loc": ".".join(map(str, error["loc"])),
                "msg": error["msg"],
                "input": json.dumps(error["input"])
            }
            for error in exc.errors(include_url=False)
        ]

    return []

return model_validate(model_class, data)

$$ LANGUAGE plpython3u;

{% endmacro %}
