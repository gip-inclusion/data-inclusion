import io
import json
import tarfile
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

pivots_of_interest = [
    "agefiph",
    "agence_insertion",
    "adil",
    "ars",
    "aav",
    "afpa",
    "apec",
    "aract",
    "apecita",
    "bav",
    "caf",
    "cicas",
    "cio",
    "cidf",
    "pmi",
    "dr_femmes",
    "ars_antenne",
    "direccte",
    "direccte_ut",
    "drihl",
    "drihl_ut",
    "dd_femmes",
    "msap",
    "greta",
    "cij",
    "epci",
    "mairie",
    "mairie_com",
    "maison_emploi",
    "mjd",
    "maison_handicapees",
    "mission_locale",
    "msa",
    "permanence_juridique",
    "plateforme_naturalisation",
    "pcb",
    "pif",
    "prefecture",
    "droit_travail",
]


def extract_data(src: str, **kwargs) -> dict[str, io.BytesIO]:
    response = requests.get(src)
    return {"data.tar.bz2": io.BytesIO(response.content)}


def read_data(path: Path) -> tuple[pd.DataFrame, Optional[pd.Series]]:
    with tarfile.open(path, "r:bz2") as tar:
        tar.extractall(path=path.parent)

    with next(path.parent.glob("*.gouv_local.json")).open() as f:
        data = json.load(f)

    df = pd.json_normalize(data["service"], max_level=0)
    return df, df.id
