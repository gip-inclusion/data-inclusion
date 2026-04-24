#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "openai",
#     "pandas",
#     "polars",
#     "pyarrow",
#     "tabulate",
#     "typer",
# ]
# ///

import os
from datetime import datetime, timedelta
from decimal import Decimal
from functools import cached_property
from pathlib import Path
from typing import Annotated, Literal

import openai
import polars as pl
import polars.selectors as cs
import tabulate
import typer

Models = Literal["claude-sonnet-4-6"]

app = typer.Typer()


INSTRUCTIONS_PROMPT = (Path(__file__).parent / "instructions.md").open().read()

SUMMARY_TEMPLATE = """
## Summary

{}

## Changes by Column

{}

## Samples of changes

{}
"""


def read(
    path: Path,
    pk_col: str = "id",
    exclude_cols: list[str] | None = None,
) -> pl.DataFrame:
    if exclude_cols is None:
        exclude_cols = []
    match path.suffix:
        case ".parquet":
            df = pl.read_parquet(path)
        case ".json":
            df = pl.read_json(path)
        case _:
            raise ValueError(f"Unsupported file format: {path.suffix}")

    df = df.drop(exclude_cols)
    df = df.sort(by=pk_col)
    df = df.with_columns(cs.list().map_elements(set))
    return df


def to_string(df: pl.DataFrame) -> str:
    return tabulate.tabulate(
        tabular_data=df.to_dicts(),
        headers="keys",
        tablefmt="github",
        maxcolwidths=100,
    )


class TimeDeltaTolerance:
    def __init__(self, tolerance: timedelta) -> None:
        self.tolerance = tolerance

    def __call__(self, before: datetime, after: datetime) -> tuple[bool, timedelta]:
        delta = abs(after - before)
        return delta < self.tolerance, delta


class RelativeTolerance:
    def __init__(self, tolerance: float) -> None:
        self.tolerance = tolerance

    def __call__(self, before: float, after: float) -> tuple[bool, float]:
        delta = abs(Decimal(str(after)) - Decimal(str(before)))
        return delta < Decimal(str(self.tolerance)), float(delta)


class ThresholdTolerance:
    def __init__(self, threshold: float) -> None:
        self.threshold = threshold

    def __call__(self, before: float, after: float) -> tuple[bool, float]:
        delta = abs(Decimal(str(after)) - Decimal(str(before)))
        return (before < self.threshold) ^ (after >= self.threshold), float(delta)


class Diff:
    added: pl.DataFrame
    removed: pl.DataFrame
    changed: pl.DataFrame
    unchanged: pl.DataFrame

    def __init__(
        self,
        before_df: pl.DataFrame,
        after_df: pl.DataFrame,
        pk_col: str = "id",
        meta_cols: list[str] | None = None,
        tolerances: dict[
            cs.Selector, TimeDeltaTolerance | RelativeTolerance | ThresholdTolerance
        ]
        | None = None,
    ):
        self.meta_cols = meta_cols if meta_cols is not None else []
        self.pk_col = pk_col

        if tolerances is None:
            tolerances = {
                cs.float(): RelativeTolerance(0.2),
                cs.date() | cs.datetime(): TimeDeltaTolerance(timedelta(weeks=4)),
            }

        self._compare(
            before_df=before_df,
            after_df=after_df,
            tolerances=tolerances if tolerances is not None else {},
        )

    def _compare(
        self,
        before_df: pl.DataFrame,
        after_df: pl.DataFrame,
        tolerances: dict[
            cs.Selector, TimeDeltaTolerance | RelativeTolerance | ThresholdTolerance
        ],
    ):
        self.added = after_df.join(before_df, on=self.pk_col, how="anti").select(
            self.pk_col, *self.meta_cols
        )
        self.removed = before_df.join(after_df, on=self.pk_col, how="anti").select(
            self.pk_col, *self.meta_cols
        )

        tolerances = tolerances if tolerances is not None else {}
        tolerance_by_column = {
            col: tol
            for selector, tol in tolerances.items()
            for col in before_df.select(selector).columns
        }

        def check_delta(row) -> dict:
            if type(row["before"]) is not type(row["after"]):
                return {"delta": None, "within_tolerance": False}

            if row["column"] in tolerance_by_column:
                tol_fn = tolerance_by_column[row["column"]]
                within_tolerance, delta = tol_fn(row["before"], row["after"])
                return {"delta": str(delta), "within_tolerance": within_tolerance}

            return {"delta": None, "within_tolerance": False}

        # find changes using pandas.DataFrame.compare
        # polars does not have a built-in equivalent
        compare_df = (
            before_df.join(after_df, on=self.pk_col, how="semi")
            .to_pandas()
            .set_index([self.pk_col, *self.meta_cols])
            .melt(ignore_index=False, var_name="column")
            .set_index("column", append=True)
            .compare(
                other=after_df.join(before_df, on=self.pk_col, how="semi")
                .to_pandas()
                .set_index([self.pk_col, *self.meta_cols])
                .melt(ignore_index=False, var_name="column")
                .set_index("column", append=True),
                result_names=("before", "after"),
            )
            .droplevel(level=0, axis=1)
            .fillna("")
            .replace({"": None})
        )

        # flag changes within tolerance zone
        compare_df = (
            compare_df.reset_index(level="column")
            .assign(
                **compare_df.reset_index(level="column").apply(
                    check_delta,
                    axis="columns",
                    result_type="expand",
                )
            )
            .set_index("column", append=True)
        )

        if len(compare_df) > 0:
            changes_df = pl.from_pandas(
                data=compare_df.astype({"before": str, "after": str}).reset_index(),
                include_index=True,
            ).filter(~pl.col("within_tolerance"))
        else:
            changes_df = pl.DataFrame(
                schema={
                    self.pk_col: str,
                    **{col: str for col in self.meta_cols},
                    "column": str,
                    "before": str,
                    "after": str,
                    "delta": str,
                    "within_tolerance": bool,
                }
            )

        self.changed = changes_df.group_by([self.pk_col, *self.meta_cols]).agg(
            pl.struct(pl.all()).alias("changes")
        )

        self.unchanged = (
            after_df.join(before_df, on=self.pk_col, how="semi")
            .join(changes_df.select(self.pk_col), on=self.pk_col, how="anti")
            .select(self.pk_col, *self.meta_cols)
            .unique()
        )

    @cached_property
    def count_rows_by_change_type(self) -> pl.DataFrame:
        return (
            pl.concat(
                [
                    self.added.with_columns(change_type=pl.lit("added")),
                    self.removed.with_columns(change_type=pl.lit("removed")),
                    self.changed.with_columns(change_type=pl.lit("modified")),
                    self.unchanged.with_columns(change_type=pl.lit("unchanged")),
                ],
                how="diagonal",
            )
            .group_by(*self.meta_cols, "change_type")
            .len()
            .pivot(on="change_type", index=self.meta_cols)
            .match_to_schema(
                {col: pl.String for col in self.meta_cols}
                | {
                    "change_type": pl.String,
                    "added": pl.UInt32,
                    "removed": pl.UInt32,
                    "modified": pl.UInt32,
                    "unchanged": pl.UInt32,
                },
                missing_columns="insert",
            )
            .sort(by=self.meta_cols)
            .select(*self.meta_cols, "added", "removed", "modified", "unchanged")
        )

    @cached_property
    def count_changes_by_column(self) -> pl.DataFrame:
        return (
            self.changed.explode("changes")
            .unnest("changes")
            .group_by(*self.meta_cols, "column")
            .len()
            .pivot(on="column", index=self.meta_cols)
            .sort(by=self.meta_cols)
        )

    @cached_property
    def samples(self) -> pl.DataFrame:
        min_changes = 100
        top_changes = 3

        return (
            self.changed.explode("changes")
            .unnest("changes")
            .filter((pl.len() >= min_changes).over(*self.meta_cols, "column"))
            .group_by(*self.meta_cols, "column", "after")
            .agg(pl.len().alias("count"))
            .sort(["column", "count"], descending=[False, True])
            .with_columns(
                pl.col("count")
                .rank("ordinal", descending=True)
                .over(*self.meta_cols, "column")
                .alias("rank")
            )
            .filter(pl.col("rank") <= top_changes)
            .select(
                *self.meta_cols,
                "column",
                pl.col("after").str.slice(0, 200),
                "count",
            )
        )

    def ai_summarize(
        self,
        model: Models = "claude-sonnet-4-6",
    ) -> str | None:
        summary_str = SUMMARY_TEMPLATE.format(
            to_string(self.count_rows_by_change_type),
            to_string(self.count_changes_by_column),
            to_string(self.samples),
        )

        openai_client = openai.OpenAI(
            api_key=os.environ.get("OPENAI_API_KEY"),
            base_url=os.environ.get("OPENAI_BASE_URL", None),
        )

        response = openai_client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": INSTRUCTIONS_PROMPT},
                {"role": "user", "content": summary_str},
            ],
            max_completion_tokens=1000,
        )

        return response.choices[0].message.content

    def summarize(
        self,
        llm: bool,
        model: Models = "claude-sonnet-4-6",
    ) -> str:
        sections = [
            ("## Par type de changement", to_string(self.count_rows_by_change_type)),
            ("## Par colonne", to_string(self.count_changes_by_column)),
        ]

        if not self.samples.is_empty():
            sections.append(("## Échantillons", to_string(self.samples)))

        if llm and (llm_summary := self.ai_summarize(model=model)) is not None:
            sections.append(("## Analyse LLM", llm_summary))

        return "\n\n---\n\n".join(["\n\n".join(section) for section in sections])


@app.command(name="compare")
def _compare(
    before: Annotated[Path, typer.Argument(help="Path to the 'before' parquet file")],
    after: Annotated[Path, typer.Argument(help="Path to the 'after' parquet file")],
    *,
    meta_cols: Annotated[
        list[str],
        typer.Option(
            help=(
                "List of metadata columns that will be retained for comparison and "
                "included in the output."
            ),
        ),
    ] = [],
    exclude_cols: Annotated[
        list[str],
        typer.Option(
            help=(
                "List of columns to exclude from the comparison. Can be used to "
                "remove non-deterministic columns like timestamps."
            ),
        ),
    ] = [],
    pk_col: Annotated[
        str, typer.Option(help="Name of the primary key column used to match rows.")
    ] = "id",
    llm: Annotated[
        bool,
        typer.Option(
            help="Whether to generate a natural language summary of the diff.",
        ),
    ] = False,
    model: Annotated[
        Models,
        typer.Option(
            help=(
                "LLM model to use for generating the summary. "
                "Ignored if --llm is not set."
            ),
        ),
    ] = "claude-sonnet-4-6",
):
    """Compare two parquet files and output the differences."""

    before_df = read(path=before, pk_col=pk_col, exclude_cols=exclude_cols)
    after_df = read(path=after, pk_col=pk_col, exclude_cols=exclude_cols)

    diff = Diff(
        before_df=before_df,
        after_df=after_df,
        pk_col=pk_col,
        meta_cols=meta_cols,
    )

    typer.echo(diff.summarize(llm=llm, model=model))


if __name__ == "__main__":
    app()
