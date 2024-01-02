import json
import tarfile
from pathlib import Path

import pandas as pd


def read(path: Path) -> pd.DataFrame:
    with tarfile.open(path, "r:bz2") as tar:
        tar.extractall(path=path.parent)

    with next(path.parent.glob("*.gouv_local.json")).open() as f:
        data = json.load(f)

    return pd.json_normalize(data["service"], max_level=0)
