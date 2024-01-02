import json
import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import trafilatura

logger = logging.getLogger(__name__)


def html_to_markdown(s: Optional[str]) -> Optional[str]:
    if s is None or s == "":
        return s
    return trafilatura.extract(trafilatura.load_html("<html>" + s + "</html>"))


def read(path: Path) -> pd.DataFrame:
    # utils.read_json is enough
    # but this adds the conversion of descriptions from html to markdown
    # should eventually be implemented as a python dbt model

    with path.open() as file:
        data = json.load(file)

    for creche_data in data:
        creche_data["details"]["presentation"]["structureProjet"] = html_to_markdown(
            creche_data["details"]["presentation"]["structureProjet"]
        )
        creche_data["details"]["modalite"]["conditionAdmision"] = html_to_markdown(
            creche_data["details"]["modalite"]["conditionAdmision"]
        )
        creche_data["details"]["modalite"]["modalitesInscription"] = html_to_markdown(
            creche_data["details"]["modalite"]["modalitesInscription"]
        )

    df = pd.DataFrame.from_records(data)
    df = df.replace({np.nan: None})

    return df
