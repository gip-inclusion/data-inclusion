from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

from data_inclusion.pipeline.common import dags, tasks

SOURCE_ID = "reseau-alpha"
SCHEMA_NAME = SOURCE_ID.replace("-", "_")

S3_PATH = "/".join(
    [
        "data",
        "raw",
        "{{ logical_date.in_timezone('Europe/Paris').date() }}",
        SOURCE_ID,
        "{{ run_id }}",
    ]
)


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
    retries=1,
)
def extract(to_path: str):
    import httpx
    import pandas as pd

    from data_inclusion.pipeline.common import s3

    # this is our entrypoint to the reseau-alpha data
    url = "https://www.reseau-alpha.org/cartographie.json"

    cartographie_data = httpx.get(url).json()

    s3.to_s3(path=f"{to_path}/cartographie.json", data=cartographie_data)

    structures_df = pd.json_normalize(cartographie_data["structures"])

    # also fetch the HTML pages for each structure
    # because we need extra data unavailable in the JSON API
    for _, row in structures_df.iterrows():
        response = httpx.get(row["url"])
        s3.to_s3(
            path=f"{to_path}/structures/{row['id']}.html",
            data=response.content,
        )


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
    retries=0,
)
def load(from_path: str, schema_name: str):
    import json
    import tempfile
    from pathlib import Path

    import pandas as pd

    from airflow.providers.amazon.aws.hooks import s3 as s3_hooks
    from airflow.providers.postgres.hooks import postgres

    from data_inclusion.pipeline.common import pg, s3
    from data_inclusion.pipeline.dags.import_reseau_alpha import scraping

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")
    s3_hook = s3_hooks.S3Hook(aws_conn_id="s3")

    with s3.from_s3(path=from_path + "/cartographie.json").open() as f:
        cartographie_data = json.load(f)

    structures_df = pd.DataFrame.from_records(cartographie_data["structures"])
    pg.to_pg(
        hook=pg_hook, df=structures_df, schema_name=schema_name, table_name="structures"
    )

    pages_data = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for key in s3_hook.list_keys(prefix=from_path + "/structures"):
            path = Path(
                s3_hook.download_file(
                    key=key, local_path=tmpdir, preserve_file_name=True
                )
            )
            pages_data.append(
                {
                    "structure_id": path.stem,
                    **scraping.scrap_structure(path.read_text()),
                }
            )

    pages_df = pd.DataFrame(pages_data)
    pg.to_pg(hook=pg_hook, df=pages_df, schema_name=schema_name, table_name="pages")


@dag(
    schedule="@daily",
    **dags.common_args(use_sentry=True),
)
def import_reseau_alpha():
    chain(
        tasks.create_schema(name=SCHEMA_NAME),
        extract(to_path=S3_PATH),
        load(from_path=S3_PATH, schema_name=SCHEMA_NAME),
    )


dag = import_reseau_alpha()

if __name__ == "__main__":
    dag.test()
