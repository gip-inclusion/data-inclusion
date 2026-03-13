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
    import os
    import tarfile
    import tempfile
    from pathlib import Path

    import pandas as pd
    import s3fs
    import sqlalchemy as sqla
    from sqlalchemy.dialects.postgresql import insert

    # https://github.com/apache/airflow/issues/51422#issuecomment-4068304878
    # This bug is still active :'(
    # But it does not crash, it HANGS.
    # This is because we're using task.virtualenv
    # - In production it works because FabAuthManager loads the entire airflow
    #   vars and secrets.
    # - run_main_pipeline uses task.python which uses the airflow environment and
    #   also has access to vars and secrets.
    # - import_api_analytics uses task.virtualenv which does not have access to
    #   vars and secrets.
    engine = sqla.create_engine(os.environ["AIRFLOW_CONN_PG"])
    s3_conn = json.loads(os.environ["AIRFLOW_CONN_S3"])
    s3_extra = s3_conn["extra"]
    s3_bucket = s3_extra["service_config"]["s3"]["bucket_name"]

    s3fs_client = s3fs.S3FileSystem(
        endpoint_url=s3_extra["endpoint_url"],
        key=s3_extra["aws_access_key_id"],
        secret=s3_extra["aws_secret_access_key"],
    )

    BASE_KEY = Path(s3_bucket) / "data" / "api"
    value = sorted(s3fs_client.ls(BASE_KEY))[-1]  # latest day
    value = sorted(s3fs_client.ls(value))[-1]  # latest run
    value = Path(value) / "analytics.tar.gz"
    print(f"Using {value}")

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
                        sqla.schema.AddConstraint(
                            sqla.PrimaryKeyConstraint(table.c.id)
                        )
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
