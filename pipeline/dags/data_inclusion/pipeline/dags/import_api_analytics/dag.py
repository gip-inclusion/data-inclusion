from airflow.sdk import dag, task

from data_inclusion.pipeline.common import dags


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
    retries=1,
)
def import_data():
    import tarfile
    import tempfile
    from pathlib import Path

    import pandas as pd
    import sqlalchemy as sqla
    from sqlalchemy.dialects.postgresql import insert

    from airflow.providers.amazon.aws.fs import s3 as s3fs
    from airflow.providers.amazon.aws.hooks import s3
    from airflow.sdk import Connection

    s3_hook = s3.S3Hook(aws_conn_id="s3")
    s3fs_client = s3fs.get_fs(conn_id="s3")
    base_key = Path(s3_hook.service_config["bucket_name"]) / "data" / "api"
    value = sorted(s3fs_client.ls(base_key))[-1]
    value = sorted(s3fs_client.ls(value))[-1]
    value = Path(value) / "analytics.tar.gz"
    print(f"Using {value}")

    pg_conn = Connection.get(conn_id="pg")
    engine = sqla.create_engine(pg_conn.get_uri())

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        with tempfile.NamedTemporaryFile(suffix=".tar.gz") as tmpfile:
            s3fs_client.get_file(rpath=value, lpath=tmpfile.name)
            archive_path = Path(tmpfile.name)
            with tarfile.open(archive_path, "r:gz") as tar:
                tar.extractall(path=tmpdir_path)

        metadata = sqla.MetaData()

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

            table = sqla.Table(table_name, metadata, autoload_with=engine)
            with engine.begin() as conn:
                # dbt creates these tables without PK; ON CONFLICT needs one
                if not sqla.inspect(conn).get_pk_constraint(table_name)[
                    "constrained_columns"
                ]:
                    conn.execute(
                        sqla.schema.AddConstraint(sqla.PrimaryKeyConstraint(table.c.id))
                    )

                records = df.to_dict(orient="records")
                stmt = insert(table).values(records)
                stmt = stmt.on_conflict_do_nothing(index_elements=[table.c.id])
                result = conn.execute(stmt)
                print(f"{table_name}: {result.rowcount} rows inserted")

    print("All tables imported.")


@dag(
    schedule="45 5-22 * * *",
    **dags.common_args(use_sentry=True),
)
def import_api_analytics():
    import_data()


import_api_analytics()
