from airflow.sdk import dag, task

from data_inclusion.pipeline.common import dags


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
    retries=1,
)
def import_data():
    import json
    import tarfile
    import tempfile
    from pathlib import Path

    import pandas as pd
    import sqlalchemy as sqla
    from sqlalchemy.dialects.postgresql import insert

    from airflow.providers.amazon.aws.fs import s3 as s3fs
    from airflow.providers.amazon.aws.hooks import s3
    from airflow.sdk import Connection

    ARRAY_COLUMNS = {
        "frais",
        "modes_accueil",
        "publics",
        "reseaux_porteurs",
        "sources",
        "thematiques",
        "types",
    }
    CHUNK_SIZE = 10_000

    def to_python_list(value):
        if hasattr(value, "tolist"):
            return value.tolist()
        if isinstance(value, str) and value and value[0] == "[":
            return json.loads(value)
        return value

    pg_conn = Connection.get(conn_id="pg")
    s3_hook = s3.S3Hook(aws_conn_id="s3")
    s3fs_client = s3fs.get_fs(conn_id="s3")
    engine = sqla.create_engine(
        pg_conn.get_uri().replace(
            "postgres://",
            "postgresql://",
        )
    )

    BASE_KEY = Path(s3_hook.service_config["bucket_name"]) / "data" / "api"
    value = sorted(s3fs_client.ls(BASE_KEY))[-1]  # latest day
    value = sorted(s3fs_client.ls(value))[-1]  # latest run
    value = Path(value) / "analytics.tar.gz"
    print(f"Using {value}")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        with tempfile.NamedTemporaryFile(suffix=".tar.gz") as tmpfile:
            s3fs_client.get_file(rpath=str(value), lpath=tmpfile.name)
            archive_path = Path(tmpfile.name)
            with tarfile.open(archive_path, "r:gz") as tar:
                tar.extractall(path=tmpdir_path)

        for table_name in [
            "api__consult_structure_events_v1",
            "api__consult_service_events_v1",
            "api__list_services_events_v1",
            "api__list_structures_events_v1",
            "api__search_services_events_v1",
        ]:
            df = pd.read_parquet(tmpdir_path / f"{table_name}.parquet")
            if df.empty:
                print(f"empty parquet file={table_name}, skipping")
                continue

            total_rows = 0
            with engine.begin() as conn:
                conn.execute(
                    sqla.text(
                        "CREATE UNIQUE INDEX IF NOT EXISTS"
                        f" {table_name}_id_idx ON {table_name} (id)"
                    )
                )
                for start in range(0, len(df), CHUNK_SIZE):
                    chunk = df.iloc[start : start + CHUNK_SIZE].copy()
                    for col in chunk.columns:
                        if col in ARRAY_COLUMNS:
                            chunk[col] = chunk[col].map(to_python_list)
                    rows = chunk.to_sql(
                        name=table_name,
                        con=conn,
                        if_exists="append",
                        index=False,
                        method=lambda table, conn, keys, iterator: (
                            conn.execute(
                                insert(table.table)
                                .values([dict(zip(keys, row)) for row in iterator])
                                .on_conflict_do_nothing(index_elements=["id"])
                            ).rowcount
                        ),
                    )
                    total_rows += rows or 0

            print(f"{table_name}: {total_rows} rows inserted")

    print("All tables imported.")


@dag(
    schedule="45 5-22 * * *",
    **dags.common_args(use_sentry=True),
)
def import_api_analytics():
    import_data()


import_api_analytics()
