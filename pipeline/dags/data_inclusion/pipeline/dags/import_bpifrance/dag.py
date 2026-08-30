from airflow.sdk import chain, dag, task

from data_inclusion.pipeline.common import dags, s3, tasks
from data_inclusion.pipeline.dags.import_bpifrance import constants

S3_PATH = s3.get_key_for_raw_data(source_id=constants.SOURCE_ID)


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
    retries=1,
)
def extract(to_path: str):
    from pathlib import Path

    from data_inclusion.pipeline.common import s3
    from data_inclusion.pipeline.dags.import_bpifrance import constants, utils

    bpifrance_client = utils.BpiClient(base_url=constants.API_URL)
    for filename, extract_fn in [
        (constants.STRUCTURES_FILENAME, bpifrance_client.list_structures),
        (constants.SERVICES_FILENAME, bpifrance_client.list_services),
    ]:
        data = extract_fn()
        s3.to_s3(path=Path(to_path) / filename, data=data)


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
)
def load(schema_name: str, from_s3_path: str):
    from pathlib import Path

    import numpy as np
    import pandas as pd

    from airflow.providers.postgres.hooks import postgres

    from data_inclusion.pipeline.common import pg, s3
    from data_inclusion.pipeline.dags.import_bpifrance import constants

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    for filename in [constants.STRUCTURES_FILENAME, constants.SERVICES_FILENAME]:
        tmp_path = s3.from_s3(path=Path(from_s3_path) / filename)
        df = pd.read_json(path_or_buf=tmp_path, dtype=False).replace({np.nan: None})
        pg.to_pg(
            hook=pg_hook,
            df=df,
            schema_name=schema_name,
            table_name=filename.removesuffix(".json"),
        )


@dag(
    schedule="@daily",
    **dags.common_args(use_sentry=True),
)
def import_bpifrance():
    chain(
        tasks.create_schema(name=constants.SCHEMA_NAME),
        extract(to_path=str(S3_PATH)),
        load(
            schema_name=constants.SCHEMA_NAME,
            from_s3_path=str(S3_PATH),
        ),
    )


dag = import_bpifrance()

if __name__ == "__main__":
    dag.test()
