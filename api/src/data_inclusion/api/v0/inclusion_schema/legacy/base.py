import enum
import textwrap

import pydantic


def Field(*args, **kwargs):
    if "description" in kwargs:
        kwargs["description"] = (
            textwrap.dedent(kwargs["description"].strip("\n"))
            .replace("\n", " ")
            .strip()
        )
    return pydantic.Field(*args, **kwargs)


class BaseModel(pydantic.BaseModel):
    @classmethod
    def model_list_json_schema(
        cls,
        title: str,
        id: str,
        description: str,
    ) -> dict:
        adapter = pydantic.TypeAdapter(list[cls])
        schema = adapter.json_schema()

        return {
            "title": title,
            "$schema": "http://json-schema.org/draft-07/schema",
            "$id": id,
            "description": description,
            **schema,
        }


class EnhancedEnum(str, enum.Enum):
    def __new__(cls, value, label, description):
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj._label = label
        obj._description = (
            textwrap.dedent(description).strip("\n").replace("\n", " ")
            if description
            else None
        )

        try:
            if obj._description is not None:
                if not obj._description[0].isupper():
                    raise ValueError("description must start with uppercase letter")
        except ValueError as err:
            raise ValueError(f"{cls.__name__}({value})'s {err}")

        return obj

    @classmethod
    def as_dict_list(cls) -> list[dict]:
        return sorted(
            [
                {
                    "value": instance.value,
                    "label": instance.label,
                    "description": instance.description,
                }
                for instance in cls
            ],
            key=lambda d: d["value"],
        )

    @property
    def label(self) -> str:
        return self._label

    @property
    def description(self) -> str:
        return self._description
