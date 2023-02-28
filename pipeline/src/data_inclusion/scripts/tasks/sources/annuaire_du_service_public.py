import json
import tarfile
from pathlib import Path

import pandas as pd

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


def read(path: Path) -> pd.DataFrame:
    with tarfile.open(path, "r:bz2") as tar:
        tar.extractall(path=path.parent)

    with next(path.parent.glob("*.gouv_local.json")).open() as f:
        data = json.load(f)

    return pd.json_normalize(data["service"], max_level=0)
