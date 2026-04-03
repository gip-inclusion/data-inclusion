from datetime import datetime, timedelta
from textwrap import dedent

import polars as pl
import polars.selectors as cs
import pytest

from data_inclusion.pipeline.scripts.compare import compare


def test_compare():
    before_df = pl.DataFrame({"id": ["1", "2"], "source": ["a", "b"], "v": [10, 20]})
    after_df = pl.DataFrame({"id": ["1", "2"], "source": ["a", "b"], "v": [10, 30]})

    diff = compare.Diff(
        before_df=before_df,
        after_df=after_df,
        pk_col="id",
        meta_cols=["source"],
    )

    assert (
        diff.summarize(llm=False)
        == dedent(
            """\
            ## Par type de changement

            | source   | added   | removed   |   modified |   unchanged |
            |----------|---------|-----------|------------|-------------|
            | a        |         |           |            |           1 |
            | b        |         |           |          1 |             |

            ---

            ## Par colonne

            | source   |   v |
            |----------|-----|
            | b        |   1 |
            """
        ).strip()
    )


@pytest.mark.parametrize(
    ("tolerance", "delta", "change_detected"),
    [
        (timedelta(days=10), timedelta(days=0), False),
        (timedelta(days=10), timedelta(days=9), False),
        (timedelta(days=10), timedelta(days=10), True),
        (timedelta(days=10), timedelta(days=-9), False),
        (timedelta(days=10), timedelta(days=-10), True),
    ],
)
def test_date_tolerances(tolerance, delta, change_detected):
    before_value = datetime(2026, 1, 1)
    after_value = before_value + delta

    before_df = pl.DataFrame({"id": ["1"], "v": [before_value]})
    after_df = pl.DataFrame({"id": ["1"], "v": [after_value]})

    diff = compare.Diff(
        before_df=before_df,
        after_df=after_df,
        tolerances={cs.datetime(): tolerance},
    )

    assert diff.changed.is_empty() == (not change_detected)


@pytest.mark.parametrize(
    ("delta", "change_detected"),
    [
        (timedelta(days=6), False),
        (timedelta(days=7), True),
    ],
)
def test_date_default_tolerance(delta, change_detected):
    before_value = datetime(2026, 1, 1)
    after_value = before_value + delta

    before_df = pl.DataFrame({"id": ["1"], "v": [before_value]})
    after_df = pl.DataFrame({"id": ["1"], "v": [after_value]})

    diff = compare.Diff(before_df=before_df, after_df=after_df)

    assert diff.changed.is_empty() == (not change_detected)
