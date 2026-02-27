import enum
import io

import numpy as np
import pandas as pd

MAX_FILE_SIZE_IN_BYTES = 500_000_000  # 500MB


class FileTypes(enum.StrEnum):
    XLSX = ".xlsx"

    @classmethod
    def as_list(cls) -> list[str]:
        return [ft.value for ft in cls]


def read_xlsx(file_value) -> tuple[pd.DataFrame, pd.DataFrame]:
    expected_sheets = ("Structures", "Services")

    buf = io.BytesIO(file_value[0].contents)

    def _read(sheet_name: str) -> pd.DataFrame:
        df = pd.read_excel(
            buf,
            sheet_name=sheet_name,
            dtype=str,
            header=0,
        )
        df = df.replace({np.nan: None})
        df = df.iloc[~df.__ignore__.map(bool).fillna(False)]
        return df

    return [_read(sheet_name=sheet_name) for sheet_name in expected_sheets]


READ_FN_BY_FILETYPE = {
    FileTypes.XLSX: read_xlsx,
}


def read_file(file_value) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    filename = file_value[0].name
    filetype = "." + filename.split(".")[-1]
    read_fn = READ_FN_BY_FILETYPE[filetype]

    structures_df, services_df = read_fn(file_value)

    for col in ["reseaux_porteurs"]:
        structures_df[col] = structures_df[col].fillna("").apply(lambda s: s.split(","))

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
