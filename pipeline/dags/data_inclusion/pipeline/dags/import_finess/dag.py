from airflow.sdk import chain, dag, task

from data_inclusion.pipeline.common import dags, s3, tasks

SOURCE_ID = "finess"
SCHEMA_NAME = SOURCE_ID.replace("-", "_")

S3_PATH = s3.get_key_for_raw_data(source_id=SOURCE_ID)


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
    retries=1,
)
def extract(to_path: str):
    from pathlib import Path

    import httpx

    from data_inclusion.pipeline.common import s3
    from data_inclusion.pipeline.dags.import_finess import constants

    response = httpx.get(
        constants.DATASET_URL, follow_redirects=True
    ).raise_for_status()
    s3.to_s3(path=Path(to_path) / constants.FILENAME, data=response.content)


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
)
def load(schema_name: str, from_s3_path: str):
    from pathlib import Path

    from airflow.providers.postgres.hooks import postgres

    from data_inclusion.pipeline.common import pg, s3
    from data_inclusion.pipeline.dags.import_finess import constants, utils

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    tmp_path = s3.from_s3(path=Path(from_s3_path) / constants.FILENAME)
    df = utils.read(tmp_path)
    pg.to_pg(
        hook=pg_hook,
        df=df,
        schema_name=schema_name,
        table_name=constants.FILENAME.removesuffix(".csv"),
    )


@dag(
    schedule="@daily",
    **dags.common_args(use_sentry=True),
)
def import_finess():
    chain(
        tasks.create_schema(name=SCHEMA_NAME),
        extract(to_path=str(S3_PATH)),
        load(schema_name=SCHEMA_NAME, from_s3_path=str(S3_PATH)),
    )


dag = import_finess()

if __name__ == "__main__":
    dag.test()
