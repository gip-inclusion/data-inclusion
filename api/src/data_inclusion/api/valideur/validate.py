import pandas as pd
import pydantic


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
                    "schéma": model.__name__,
                    "champ": field,
                    "ligne": line_number,
                    "id": id,
                    "valeur": error["input"] if error["type"] != "missing" else None,
                    "message": error["msg"],
                }
            )

    return pd.DataFrame(data=errors)
