import re

import numpy as np
import pandas as pd
import pytest
from sqlalchemy.dialects.postgresql import JSONB, dialect

from data_inclusion.pipeline.common.pg import _sanitize_for_json

JSONB_BIND = JSONB().bind_processor(dialect())
NAN_TOKEN = re.compile(r"(?<=[:,\[])\s*NaN(?=[,\}\]])")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (float("nan"), None),
        (float("inf"), None),
        (np.nan, None),
        (pd.NA, None),
        (None, None),
        ("keep", "keep"),
        ({"a": np.nan, "b": [1, np.nan]}, {"a": None, "b": [1, None]}),
    ],
)
def test_sanitize_for_json(value, expected):
    assert _sanitize_for_json(value) == expected


def test_sanitize_for_json_produces_postgres_compatible_json():
    df = pd.DataFrame(
        [
            {"latitude": np.nan, "longitude": 1.0},
            {"latitude": pd.NA, "nested": {"score": np.nan}},
        ]
    )
    for row in df.apply(
        lambda row: _sanitize_for_json(row.to_dict()),
        axis="columns",
    ):
        serialized = JSONB_BIND(row)
        assert not NAN_TOKEN.search(serialized)
