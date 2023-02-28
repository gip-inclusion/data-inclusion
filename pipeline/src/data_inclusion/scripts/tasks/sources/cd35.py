from pathlib import Path

import pandas as pd

from data_inclusion.scripts.tasks import utils


def read(path: Path) -> pd.DataFrame:
    return utils.read_csv(path, sep=";")
