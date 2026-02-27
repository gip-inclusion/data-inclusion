#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "openai",
#     "pandas",
#     "pyarrow",
#     "tabulate",
#     "typer",
# ]
# ///

import os
import sys
from pathlib import Path
from typing import Annotated

import numpy as np
import openai
import pandas as pd
import tabulate
import typer

app = typer.Typer()


INSTRUCTIONS_PROMPT = (Path(__file__).parent / "instructions.md").open().read()


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


def is_significant_change(before, after) -> bool:
    if isinstance(before, pd.Timestamp) and isinstance(after, pd.Timestamp):
        return before - after > pd.Timedelta(days=7)

    if isinstance(before, (int, float)) and isinstance(after, (int, float)):
        return abs(after - before) >= 0.1 * abs(before)

    return True


def _compare(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
) -> pd.DataFrame:
    diff_df = pd.DataFrame()

    added_idx = after_df.index.difference(before_df.index)
    shared_idx = before_df.index.intersection(after_df.index)
    removed_idx = before_df.index.difference(after_df.index)

    if not added_idx.empty:
        added_df = after_df.loc[added_idx]
        added_df = added_df.melt(
            ignore_index=False, var_name="column", value_name="after"
        ).set_index("column", append=True)
        added_df = added_df.assign(before=np.nan)
        added_df = added_df.assign(change_type="added")

        diff_df = pd.concat([diff_df, added_df])

    if not removed_idx.empty:
        removed_df = before_df.loc[removed_idx]
        removed_df = removed_df.melt(
            ignore_index=False, var_name="column", value_name="before"
        ).set_index("column", append=True)
        removed_df = removed_df.assign(after=np.nan)
        removed_df = removed_df.assign(change_type="removed")

        diff_df = pd.concat([diff_df, removed_df])

    if not shared_idx.empty:
        melted_before_df = (
            before_df.loc[shared_idx]
            .melt(ignore_index=False, var_name="column")
            .set_index("column", append=True)
        )
        melted_after_df = (
            after_df.loc[shared_idx]
            .melt(ignore_index=False, var_name="column")
            .set_index("column", append=True)
        )
        modified_df = melted_before_df.compare(
            other=melted_after_df,
            keep_equal=True,
            result_names=("before", "after"),
        )
        modified_df = modified_df.set_axis(modified_df.columns.droplevel(0), axis=1)
        modified_df = modified_df.assign(change_type="modified")
        modified_df = modified_df.assign(
            change_type=modified_df.apply(
                lambda row: (
                    "modified"
                    if is_significant_change(row["before"], row["after"])
                    else "almost_unchanged"
                ),
                axis=1,
            )
        )

        diff_df = pd.concat([diff_df, modified_df])

        unchanged_idx = shared_idx.difference(modified_df.index.get_level_values("id"))

        unchanged_df = (
            before_df.loc[unchanged_idx]
            .melt(ignore_index=False, var_name="column", value_name="before")
            .set_index("column", append=True)
        )
        unchanged_df = unchanged_df.assign(change_type="unchanged")
        diff_df = pd.concat([diff_df, unchanged_df])

    diff_df = diff_df.reset_index()

    return diff_df


@app.command()
def compare(
    before: Annotated[Path, typer.Argument(help="Path to the 'before' parquet file")],
    after: Annotated[Path, typer.Argument(help="Path to the 'after' parquet file")],
    *,
    meta_columns: Annotated[
        list[str],
        typer.Option(
            help=(
                "List of metadata columns that will be retained for comparison and "
                "included in the output."
            ),
        ),
    ] = [],
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
):
    """Compare two parquet files and output the differences."""

    before_df = read(before)
    after_df = read(after)

    before_df = prepare(before_df, index_column, meta_columns, exclude_columns)
    after_df = prepare(after_df, index_column, meta_columns, exclude_columns)

    diff_df = _compare(before_df, after_df)

    typer.echo(to_json(diff_df))


def to_string(df: pd.DataFrame) -> str:
    return tabulate.tabulate(
        tabular_data=df,
        headers="keys",
        tablefmt="github",
    )


def to_json(df: pd.DataFrame) -> str:
    return df.to_json(
        orient="records",
        lines=True,
        force_ascii=True,
        date_format="iso",
        date_unit="s",
    )


def get_samples(
    diff_df: pd.DataFrame,
    meta_columns: list[str],
) -> pd.DataFrame:
    group_by = meta_columns + ["column"]
    minimum_group_size = 100
    samples_by_group = 20

    diff_df = diff_df[diff_df["change_type"] == "modified"]
    diff_df = diff_df[meta_columns + ["column", "before", "after"]]
    diff_df = diff_df.groupby(group_by).filter(lambda g: len(g) >= minimum_group_size)
    diff_df = diff_df.groupby(["source", "column"]).sample(n=samples_by_group)
    diff_df = diff_df.reset_index(drop=True)

    return diff_df


SUMMARY_TEMPLATE = """
## Summary

{}

## Changes by Column

{}

## Samples of changes

{}
"""


def _summarize(
    diff_df: pd.DataFrame,
    meta_columns: list[str],
) -> str:
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
            index=[count_by_type_df[col] for col in meta_columns]
            if meta_columns
            else pd.Series("all", index=count_by_type_df.index),
            columns=count_by_type_df["change_type"],
            values=count_by_type_df["count"],
            aggfunc="sum",
            dropna=False,
        )
        .fillna(0)
        .astype(int)
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
            index=[count_by_column_df[col] for col in meta_columns]
            if meta_columns
            else pd.Series("all", index=count_by_column_df.index),
            columns=count_by_column_df["column"],
            values=count_by_column_df["count"],
            aggfunc="sum",
            dropna=False,
        )
        .fillna(0)
        .astype(int)
        .replace({0: ""})
    )

    return SUMMARY_TEMPLATE.format(
        to_string(count_by_type_df),
        to_string(count_by_column_df),
        to_string(get_samples(diff_df, meta_columns)),
    )


def get_llm_summary(
    openai_client: openai.OpenAI,
    summary_str: str,
    model: str,
) -> str:
    response = openai_client.responses.create(
        model=model,
        instructions=INSTRUCTIONS_PROMPT,
        input=summary_str,
    )
    return response.output_text


@app.command()
def summarize(
    input_file: Annotated[
        typer.FileText,
        typer.Argument(
            help="Path to the diff output file (JSONL format).",
            default_factory=lambda: sys.stdin,
            show_default="sys.stdin",
        ),
    ],
    *,
    llm: Annotated[
        bool,
        typer.Option(
            help="Whether to generate a natural language summary of the diff.",
        ),
    ] = False,
    model: Annotated[
        str,
        typer.Option(
            help=(
                "LLM model to use for generating the summary. "
                "Ignored if --llm is not set."
            ),
        ),
    ] = "gpt-5.2",
):
    """Summarize the diff output from the compare command."""

    diff_df = pd.read_json(input_file, orient="records", lines=True, dtype=False)

    expected_columns = {
        "id",
        "column",
        "before",
        "after",
        "change_type",
    }
    meta_columns = [col for col in diff_df.columns if col not in expected_columns]

    summary_str = _summarize(diff_df, meta_columns)
    typer.echo(summary_str)

    if llm:
        openai_client = openai.OpenAI(
            api_key=os.environ.get("OPENAI_API_KEY"),
            base_url=os.environ.get("OPENAI_BASE_URL", None),
        )
        llm_summary = get_llm_summary(openai_client, summary_str, model)
        typer.echo(llm_summary)


if __name__ == "__main__":
    app()
