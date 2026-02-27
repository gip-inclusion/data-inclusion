import difflib
import enum
from itertools import groupby

import pydantic_core


def display_ranges(items: list[int]) -> str:
    ranges = [
        [item for _, item in group]
        for _, group in groupby(enumerate(sorted(items)), lambda t: t[0] - t[1])
    ]

    def format_range(r: list[int]) -> str:
        if len(r) == 1:
            return str(r[0])
        else:
            return f"{r[0]}-{r[-1]}"

    return ", ".join(format_range(r) for r in ranges)


def enum_error_message_with_suggestion(
    error_details: pydantic_core.ErrorDetails,
    enum_cls: type[enum.StrEnum],
) -> pydantic_core.ErrorDetails:
    if error_details["type"] != "enum":
        return error_details

    item_in_error = str(error_details["input"])

    nearest_value = next(
        iter(
            difflib.get_close_matches(
                item_in_error,
                [e.value for e in enum_cls],
                n=1,
            )
        ),
        None,
    )

    msg = f"La valeur '{item_in_error}' n'existe pas"

    if nearest_value is not None:
        msg += f". Vouliez-vous dire '{nearest_value}' ?"

    error_details["msg"] = msg

    return {
        **error_details,
        "msg": msg,
    }
