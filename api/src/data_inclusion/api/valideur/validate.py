import numpy as np
import pandas as pd
import pydantic

from data_inclusion.schema import v1


def validate_dataset(
    structures_df: pd.DataFrame,
    services_df: pd.DataFrame,
) -> pd.DataFrame:
    structures_errors_df = list_errors(v1.Structure, structures_df)
    services_errors_df = list_errors(v1.Service, services_df)
    errors_df = pd.concat([structures_errors_df, services_errors_df])
    errors_df = errors_df.replace({np.nan: None})
    return errors_df


def list_errors(model, df: pd.DataFrame) -> pd.DataFrame:
    errors = []
    data = df.to_json(path_or_buf=None, orient="records", force_ascii=False)

    try:
        pydantic.RootModel[list[model]].model_validate_json(json_data=data)
    except pydantic.ValidationError as exc:
        for error in exc.errors():
            loc = error["loc"]
            line_number, field = loc[0], loc[1]
            id = df.iloc[line_number].get("id", None)

            errors.append(
                {
                    "version": "v1",
                    "fqn": f"{model.__name__}.{field}",
                    "schema": model.__name__,
                    "field": field,
                    "line": line_number,
                    "id": id,
                    "value": error["input"] if error["type"] != "missing" else None,
                    "message": error["msg"],
                }
            )

    return pd.DataFrame(data=errors)
