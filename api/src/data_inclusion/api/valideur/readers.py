import enum
from typing import BinaryIO

import numpy as np
import pandas as pd


class FileTypes(enum.StrEnum):
    XLSX = ".xlsx"

    @classmethod
    def as_list(cls) -> list[str]:
        return [ft.value for ft in cls]


def read_xlsx(content: BinaryIO) -> tuple[pd.DataFrame, pd.DataFrame]:
    def _read(sheet_name: str) -> pd.DataFrame:
        df = pd.read_excel(
            content,
            sheet_name=sheet_name,
            dtype=str,
            header=0,
        )
        df = df.replace({np.nan: None})
        df = df.iloc[~df.__ignore__.map(bool).fillna(False)]
        return df

    return _read("Structures"), _read("Services")


READ_FN_BY_FILETYPE = {
    FileTypes.XLSX: read_xlsx,
}


def read_file(
    content: BinaryIO,
    file_type: FileTypes,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    read_fn = READ_FN_BY_FILETYPE[file_type]

    structures_df, services_df = read_fn(content)

    for col in ["reseaux_porteurs"]:
        structures_df[col] = (
            structures_df[col].fillna("").apply(lambda s: s.split(",")).str.strip()
        )

    for col in [
        "thematiques",
        "publics",
        "modes_accueil",
        "zone_eligibilite",
        "modes_mobilisation",
        "mobilisable_par",
    ]:
        services_df[col] = (
            services_df[col].fillna("").apply(lambda s: s.split(",")).str.strip()
        )

    return structures_df, services_df
