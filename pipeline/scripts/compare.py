#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "tabulate",
#     "typer",
# ]
# ///

import sys
from pathlib import Path
from typing import Annotated

import numpy as np
import pandas as pd
import tabulate
import typer

app = typer.Typer()


def read(path: Path) -> pd.DataFrame:
    match path.suffix:
        case ".parquet":
            df = pd.read_parquet(path)
        case ".json":
            df = pd.read_json(path, dtype=False)
        case _:
            raise ValueError(f"Unsupported file format: {path.suffix}")

    return df.replace({np.nan: None})


def prepare(
    df: pd.DataFrame,
    index_column: str,
    meta_columns: list[str],
    exclude_columns: list[str],
) -> pd.DataFrame:
    df = df.drop(columns=exclude_columns)

    # convert list-like columns to sets for better comparison
    for column in df.columns:
        is_list_like = (
            df[column].dropna().map(lambda x: isinstance(x, (np.ndarray, list))).all()
        )

        if is_list_like:
            df = df.assign(
                **{
                    column: df[column].apply(
                        lambda x: set(x) if x is not None else None
                    )
                }
            )

    df = df.sort_values(by=index_column)

    # retain only the index and meta columns for comparison
    df = df.set_index(keys=meta_columns + [index_column])

    return df


def compare(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
) -> pd.DataFrame:
    diff_df = pd.DataFrame()

    added_idx = after_df.index.difference(before_df.index)
    modified_idx = before_df.index.intersection(after_df.index)
    removed_idx = before_df.index.difference(after_df.index)

    if not added_idx.empty:
        added_df = after_df.loc[added_idx]
        added_df = added_df.stack()
        added_df = added_df.reindex(added_df.index.set_names("column", level=-1))
        added_df = added_df.to_frame(name="after")
        added_df = added_df.assign(before=np.nan)
        added_df = added_df.assign(change_type="added")

        diff_df = pd.concat([diff_df, added_df])

    if not removed_idx.empty:
        removed_df = before_df.loc[removed_idx]
        removed_df = removed_df.stack()
        removed_df = removed_df.reindex(removed_df.index.set_names("column", level=-1))
        removed_df = removed_df.to_frame(name="before")
        removed_df = removed_df.assign(after=np.nan)
        removed_df = removed_df.assign(change_type="removed")

        diff_df = pd.concat([diff_df, removed_df])

    if not modified_idx.empty:
        modified_df = before_df.loc[modified_idx].compare(
            other=after_df.loc[modified_idx],
            keep_equal=False,
            keep_shape=False,
            result_names=("before", "after"),
        )
        modified_df = modified_df.stack(level=0)
        modified_df = modified_df.reindex(
            modified_df.index.set_names("column", level=-1)
        )

        if isinstance(modified_df, pd.Series):
            modified_df = modified_df.to_frame()

        modified_df = modified_df.dropna(how="all")
        modified_df = modified_df.assign(change_type="modified")

        diff_df = pd.concat([diff_df, modified_df])

    diff_df = diff_df.reset_index()

    return diff_df


def print_diff_summary(
    diff_df: pd.DataFrame,
    meta_columns: list[str],
) -> None:
    count_by_type_df = (
        diff_df.groupby(
            by=meta_columns + ["change_type"],
            dropna=False,
        )["id"]
        .nunique()
        .rename("count")
        .reset_index()
    )
    count_by_type_df = (
        pd.crosstab(
            index=[count_by_type_df[col] for col in meta_columns],
            columns=count_by_type_df["change_type"],
            values=count_by_type_df["count"],
            aggfunc="sum",
            dropna=False,
        )
        .fillna(0)
        .astype(int)
    )
    typer.echo(
        tabulate.tabulate(
            count_by_type_df,
            headers="keys",
            tablefmt="grid",
        )
    )

    count_by_column_df = (
        diff_df.iloc[diff_df["change_type"] == "modified"]
        .value_counts(
            meta_columns + ["column"],
            dropna=False,
        )
        .reset_index(name="count")
    )
    count_by_column_df = (
        pd.crosstab(
            index=[count_by_column_df[col] for col in meta_columns],
            columns=count_by_column_df["column"],
            values=count_by_column_df["count"],
            aggfunc="sum",
            dropna=False,
        )
        .fillna(0)
        .astype(int)
        .replace({0: ""})
    )
    typer.echo(
        tabulate.tabulate(
            count_by_column_df,
            headers="keys",
            tablefmt="grid",
        )
    )


@app.command()
def main(
    before: Annotated[Path, typer.Argument(help="Path to the 'before' parquet file")],
    after: Annotated[Path, typer.Argument(help="Path to the 'after' parquet file")],
    meta_columns: Annotated[
        list[str],
        typer.Option(
            help=(
                "List of metadata columns that will be retained for comparison and "
                "included in the output."
            ),
        ),
    ] = ["source"],
    exclude_columns: Annotated[
        list[str],
        typer.Option(
            help=(
                "List of columns to exclude from the comparison. Can be used to "
                "removed non-deterministic columns like timestamps."
            ),
        ),
    ] = [],
    index_column: Annotated[
        str, typer.Option(help="Name of the index column used to match rows.")
    ] = "id",
    full: Annotated[
        bool,
        typer.Option(
            help="Output the full diff instead of a summary.",
        ),
    ] = False,
):
    """Compare two parquet files and output the differences."""

    before_df = read(before)
    after_df = read(after)

    before_df = prepare(before_df, index_column, meta_columns, exclude_columns)
    after_df = prepare(after_df, index_column, meta_columns, exclude_columns)

    diff_df = compare(before_df, after_df)

    if full:
        diff_df.to_json(
            sys.stdout,
            orient="records",
            lines=True,
            force_ascii=True,
        )
    else:
        print_diff_summary(diff_df, meta_columns)


if __name__ == "__main__":
    app()
