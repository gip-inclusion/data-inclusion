import json
import logging
from pathlib import Path

from . import utils

logger = logging.getLogger(__name__)


def read(path: Path):
    import pandas as pd

    # utils.read_json is enough
    # but this adds the conversion of descriptions from html to markdown
    # should eventually be implemented as a python dbt model

    with path.open() as file:
        data = json.load(file)

    for creche_data in data:
        creche_data["details"]["presentation"][
            "structureProjet"
        ] = utils.html_to_markdown(
            creche_data["details"]["presentation"]["structureProjet"]
        )
        creche_data["details"]["modalite"][
            "conditionAdmision"
        ] = utils.html_to_markdown(
            creche_data["details"]["modalite"]["conditionAdmision"]
        )
        creche_data["details"]["modalite"][
            "modalitesInscription"
        ] = utils.html_to_markdown(
            creche_data["details"]["modalite"]["modalitesInscription"]
        )

    df = pd.DataFrame.from_records(data)
    return utils.df_clear_nan(df)
