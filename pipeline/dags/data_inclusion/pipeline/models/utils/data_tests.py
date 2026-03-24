import pointblank as pb
import polars as pl


def relationships(
    left: pl.DataFrame,
    right: pl.DataFrame,
    left_on: pl.Expr,
    right_on: pl.Expr,
) -> pl.DataFrame:
    left = left.with_columns(left_on.alias("left_on"))
    right = right.with_columns(right_on.alias("right_on"))

    return left.join(
        other=right,
        left_on=pl.col("left_on"),
        right_on=pl.col("right_on"),
        how="left",
        coalesce=False,
    ).select(pl.col("right_on").is_not_null())


def print_report(validation: pb.Validate) -> None:
    """Workaround to show report in Airflow logs."""

    # TODO: find a better way to show the report (saving it as an html file somewhere
    # and linking to it in the logs for example)

    report_df = pl.read_json(
        validation.get_json_report(exclude_fields=["values"]).encode(),
    ).select("column", "assertion_type", "all_passed")

    with pl.Config(
        tbl_rows=-1,
        tbl_cols=-1,
        tbl_width_chars=-1,
    ):
        print(report_df)


def assert_passing(validation: pb.Validate) -> None:
    validation.interrogate()
    print_report(validation)
    assert validation.all_passed()
