import pendulum

from airflow.decorators import dag, task
from airflow.operators import empty

from dag_utils import date, sentry
from dag_utils.virtualenvs import PYTHON_BIN_PATH


@task.external_python(python=str(PYTHON_BIN_PATH))
def import_dataset(
    run_id: str,
    logical_date,
):
    import pandas as pd

    from airflow.models import Variable
    from airflow.providers.amazon.aws.hooks import s3

    from dag_utils import pg

    ODSPEP_S3_KEY_PREFIX = Variable.get("ODSPEP_S3_KEY_PREFIX")

    s3_hook = s3.S3Hook(aws_conn_id="s3_sources")

    updated_wb_filename = s3_hook.download_file(
        key="sources/odspep/2025-07-31/services_partenaires_41.xlsx"
    )
    updated_sheets = pd.read_excel(
        updated_wb_filename,
        sheet_name=None,
        dtype=str,
        engine="openpyxl",
    )

    pg.create_schema("odspep")

    def print_detailed_row_changes(df_old, df_new, id_col):
        old_indexed = df_old.set_index(id_col)
        new_indexed = df_new.set_index(id_col)

        common_ids = old_indexed.index.intersection(new_indexed.index)

        print("=== DETAILED ROW MODIFICATIONS ===")
        modifications_found = False

        for idx in common_ids:
            old_row = old_indexed.loc[idx]
            new_row = new_indexed.loc[idx]

            changes = []

            all_columns = set(old_row.index) | set(new_row.index)

            for col in all_columns:
                old_val = old_row.get(col) if col in old_row.index else None
                new_val = new_row.get(col) if col in new_row.index else None

                if col not in old_row.index:
                    changes.append(f"  📝 {col}: [NEW] → '{new_val}'")
                elif col not in new_row.index:
                    changes.append(f"  🗑️ {col}: '{old_val}' → [REMOVED]")
                else:
                    if pd.isna(old_val) and pd.isna(new_val):
                        continue  # Both NaN, no change
                    elif pd.isna(old_val) and not pd.isna(new_val):
                        changes.append(f"  ✏️ {col}: [NULL] → '{new_val}'")
                    elif not pd.isna(old_val) and pd.isna(new_val):
                        changes.append(f"  ✏️ {col}: '{old_val}' → [NULL]")
                    elif old_val != new_val:
                        changes.append(f"  ✏️ {col}: '{old_val}' → '{new_val}'")

            if changes:
                print(f"\n🔄 ID {idx}:")
                for change in changes:
                    print(change)
                modifications_found = True

        if not modifications_found:
            print("No modifications found in existing rows.")

    for excel_file_name in s3_hook.list_keys(prefix=ODSPEP_S3_KEY_PREFIX):
        tmp_filename = s3_hook.download_file(key=excel_file_name)

        df = pd.read_excel(tmp_filename, dtype=str, engine="openpyxl")

        sheet_name = excel_file_name.split("/")[-1].rstrip(".xlsx")
        updated_sheet = updated_sheets.get(sheet_name)
        if updated_sheet is None:
            print(f"Sheet {sheet_name} not found in updated sheets, skipping.")
            continue
        merged_df = pd.concat([df, updated_sheet]).drop_duplicates(
            subset=[df.columns[0]],
            keep="last",
        )

        print("Merging dataframes for sheet:", sheet_name)
        print_detailed_row_changes(df, merged_df, df.columns[0])

        df = df.assign(batch_id=run_id)
        df = df.assign(logical_date=logical_date)

        table_name = (
            excel_file_name.rstrip(".xlsx")
            .split("/")[-1]
            .replace(" ", "")
            .replace("-", "_")
            .upper()
        )

        with pg.connect_begin() as conn:
            df.to_sql(
                table_name,
                con=conn,
                schema="odspep",
                if_exists="replace",
                index=False,
            )


@dag(
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    default_args=sentry.notify_failure_args(),
    schedule="@once",
    catchup=False,
    tags=["source"],
)
def import_odspep():
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    start >> import_dataset() >> end


import_odspep()
