import enum
import mimetypes
import types
import typing
from pathlib import Path
from typing import BinaryIO

import numpy as np
import pandas as pd
import pydantic
from pydantic.fields import FieldInfo

from data_inclusion.schema import v1


class ValideurException(Exception):
    pass


class SupportedFileTypes(enum.StrEnum):
    XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _is_array_field(field_info: FieldInfo) -> bool:
    type_annotation = field_info.annotation

    if type_annotation is None:
        return False

    def _get_type(t: type) -> type:
        if isinstance(t, types.UnionType):
            # get first non-None type from the union
            return _get_type(
                next(_t for _t in typing.get_args(t) if _t is not type(None))
            )

        if (origin := getattr(t, "__origin__", None)) is not None:
            return _get_type(origin)

        return t

    return _get_type(type_annotation) in (list, set, np.ndarray)


def _get_array_fields(model: type[pydantic.BaseModel]) -> list[str]:
    """Return field names of array-like fields in the given Pydantic model."""
    return [
        fieldname
        for fieldname, field_info in model.model_fields.items()
        if _is_array_field(field_info)
    ]


def read_xlsx(file: BinaryIO | Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    def _read(
        sheet_name: str,
        model: type[v1.Structure] | type[v1.Service],
    ) -> pd.DataFrame:
        df = pd.read_excel(
            file,
            sheet_name=sheet_name,
            dtype=str,
            header=None,
        )
        df = df.replace({np.nan: None})
        # update index to start at 1 instead of 0, to match line numbers in Excel
        df = df.set_axis(labels=df.index + 1, axis=0)
        # use first row as column names, and drop it from the data
        df = df[1:].rename(columns=df.iloc[0].to_dict())
        # create a line column from the index
        df = df.reset_index(drop=False, names=["line"])
        # drop description and example rows
        df = df.iloc[~df["__ignore__"].map(bool).fillna(False)]

        # convert array-like fields from comma-separated strings to lists
        for fieldname in _get_array_fields(model):
            if fieldname in df.columns:
                df[fieldname] = (
                    df[fieldname]
                    .map(lambda s: s.split(",") if s else None)
                    .map(lambda lst: list(map(str.strip, lst)) if lst else None)
                )

        return df

    return _read("Structures", v1.Structure), _read("Services", v1.Service)


_READ_FN_BY_FILETYPE = {
    SupportedFileTypes.XLSX: read_xlsx,
}


def read_file(
    file: BinaryIO | Path,
    filename: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if isinstance(file, Path):
        filename = file.name

    if filename is None:
        raise ValideurException("filename must be provided when file is a BinaryIO")

    mime_type, _ = mimetypes.guess_type(filename)

    if mime_type is None:
        raise ValideurException(
            f"Could not determine file type from filename: {filename}"
        )

    if mime_type not in SupportedFileTypes:
        raise ValideurException(f"Unsupported file type: {mime_type}")

    return _READ_FN_BY_FILETYPE[SupportedFileTypes(mime_type)](file)
