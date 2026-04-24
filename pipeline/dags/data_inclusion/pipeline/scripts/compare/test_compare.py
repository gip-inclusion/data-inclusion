from datetime import datetime, timedelta
from decimal import Decimal
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
        tolerances={cs.datetime(): compare.TimeDeltaTolerance(tolerance)},
    )

    assert diff.changed.is_empty() == (not change_detected)


@pytest.mark.parametrize(
    ("delta", "change_detected"),
    [
        (timedelta(weeks=3, days=6), False),
        (timedelta(weeks=4), True),
    ],
)
def test_date_default_tolerance(delta, change_detected):
    before_value = datetime(2026, 1, 1)
    after_value = before_value + delta

    before_df = pl.DataFrame({"id": ["1"], "v": [before_value]})
    after_df = pl.DataFrame({"id": ["1"], "v": [after_value]})

    diff = compare.Diff(before_df=before_df, after_df=after_df)

    assert diff.changed.is_empty() == (not change_detected)


def float_delta_tolerance_factory(tol: float):
    def tol_fn(before: float, after: float) -> bool:
        return abs(Decimal(str(after)) - Decimal(str(before))) < Decimal(str(tol))

    return tol_fn


@pytest.mark.parametrize(
    ("tolerance", "delta", "change_detected"),
    [
        (0.2, 0.1, False),
        (0.2, 0.2, True),
        (0.2, -0.1, False),
        (0.2, -0.2, True),
    ],
)
def test_float_tolerance_max_delta(tolerance, delta, change_detected):
    before_value = 10.0
    after_value = 10.0 + delta

    before_df = pl.DataFrame({"id": ["1"], "v": [before_value]})
    after_df = pl.DataFrame({"id": ["1"], "v": [after_value]})

    diff = compare.Diff(
        before_df=before_df,
        after_df=after_df,
        tolerances={cs.float(): compare.RelativeTolerance(tolerance)},
    )

    assert diff.changed.is_empty() == (not change_detected)


@pytest.mark.parametrize(
    ("threshold", "before_value", "after_value", "change_detected"),
    [
        (0.8, 0.5, 0.8, True),
        (0.8, 0.8, 0.5, True),
        (0.8, 0.5, 0.7, False),
        (0.8, 0.8, 0.8, False),
        (0.8, 0.8, 1.0, False),
    ],
)
def test_float_tolerance_threshold(
    threshold, before_value, after_value, change_detected
):
    before_df = pl.DataFrame({"id": ["1"], "v": [before_value]})
    after_df = pl.DataFrame({"id": ["1"], "v": [after_value]})

    diff = compare.Diff(
        before_df=before_df,
        after_df=after_df,
        tolerances={cs.float(): compare.ThresholdTolerance(threshold)},
    )

    assert diff.changed.is_empty() == (not change_detected)
