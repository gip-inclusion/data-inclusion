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

    from airflow.sdk import Variable

    from data_inclusion.pipeline.common import s3
    from data_inclusion.pipeline.dags.import_mes_aides import constants, utils

    mes_aides_client = utils.MesAidesClient(
        base_url=constants.API_URL,
        token=Variable.get("MES_AIDES_API_KEY"),
    )

    for ressource_name, extract_fn in [
        ("aides", mes_aides_client.list_aides),
        ("garages", mes_aides_client.list_garages),
    ]:
        data = extract_fn()
        key = (Path(to_path) / ressource_name).with_suffix(".json")
        s3.to_s3(path=key, data=data)


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
)
def load(schema_name: str, from_s3_path: str):
    from pathlib import Path

    from airflow.providers.postgres.hooks import postgres

    from data_inclusion.pipeline.common import pg, s3, utils

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    for resource_name in ["aides", "garages"]:
        key = (Path(from_s3_path) / resource_name).with_suffix(".json")
        tmp_path = s3.from_s3(path=key)
        df = utils.df_from_json(tmp_path)
        pg.to_pg(hook=pg_hook, df=df, schema_name=schema_name, table_name=resource_name)


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
