from airflow.sdk import chain, dag, task

from data_inclusion.pipeline.common import dags, s3, tasks

SOURCE_ID = "mes-aides"
SCHEMA_NAME = SOURCE_ID.replace("-", "_")

S3_PATH = s3.get_key_for_raw_data(source_id=SOURCE_ID)


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
)
def extract(to_path: str):
    from pathlib import Path

    from data_inclusion.pipeline.common import s3
    from data_inclusion.pipeline.dags.import_mes_aides import constants

    for resource_key in [
        Path(constants.AIDES_FILE_KEY),
        Path(constants.GARAGES_SOLIDAIRES_FILE_KEY),
    ]:
        with s3.from_s3(path=resource_key).open("rb") as f:
            s3.to_s3(path=Path(to_path) / resource_key.name, data=f.read())


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
)
def load(schema_name: str, from_s3_path: str):
    from pathlib import Path

    from airflow.providers.postgres.hooks import postgres

    from data_inclusion.pipeline.common import pg, utils

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    table_name = Path(from_s3_path).stem
    tmp_path = s3.from_s3(path=from_s3_path)
    df = utils.read_csv(tmp_path, sep=",")

    pg.to_pg(hook=pg_hook, df=df, schema_name=schema_name, table_name=table_name)


@dag(
    schedule="@once",
    **dags.common_args(use_sentry=True),
)
def import_mes_aides():
    chain(
        tasks.create_schema(name=SCHEMA_NAME),
        extract(to_path=str(S3_PATH)),
        load(schema_name=SCHEMA_NAME, from_s3_path=str(S3_PATH)),
    )


dag = import_mes_aides()

if __name__ == "__main__":
    dag.test()
