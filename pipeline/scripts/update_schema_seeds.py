"""Update the seeds in pipeline/seeds/schema from the latest schema version on GH"""

import os
from pathlib import Path

import pandas as pd

BASE_URL = os.environ.get(
    "BASE_URL",
    "https://raw.githubusercontent.com/betagouv/data-inclusion-schema/latest/schemas/extra/",  # noqa: E501
)
OUTPUT_DIR = Path(__file__).parent.parent / "dbt" / "seeds" / "schema"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

for tablename in [
    "frais",
    "labels-nationaux",
    "modes-accueil",
    "modes-orientation-accompagnateur",
    "modes-orientation-beneficiaire",
    "profils",
    "thematiques",
    "typologies-de-services",
    "typologies-de-structures",
]:
    df = pd.read_json(BASE_URL + tablename + ".json", dtype=False)
    df.to_csv(OUTPUT_DIR / (tablename.replace("-", "_") + ".csv"), index=False)
