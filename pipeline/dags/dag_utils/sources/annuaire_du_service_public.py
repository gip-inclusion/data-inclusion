import json
import tarfile
from pathlib import Path


# https://www.cve.org/CVERecord?id=CVE-2025-8194
# TODO: remove this patch when the issue is fixed in tarfile
def _block_patched(self, count):
    if count < 0:
        raise tarfile.InvalidHeaderError("invalid offset")
    return _block_patched._orig_block(self, count)


_block_patched._orig_block = tarfile.TarInfo._block
tarfile.TarInfo._block = _block_patched


def read(path: Path):
    import pandas as pd

    with tarfile.open(path, "r:bz2") as tar:
        tar.extractall(path=path.parent)

    with next(path.parent.glob("*.gouv_local.json")).open() as f:
        data = json.load(f)

    return pd.json_normalize(data["service"], max_level=0)
