from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

from data_inclusion.pipeline.common import dags, s3, tasks


@task.external_python(
    python=tasks.PYTHON_BIN_PATH,
    retries=2,
)
def list_cities(max_number_of_cities: int):
    """List the top cities in France by population."""
    import io
    import zipfile

    import httpx
    import pandas as pd

    url = "https://www.insee.fr/fr/statistiques/fichier/7739582/ensemble.zip"

    df = pd.read_csv(
        zipfile.ZipFile(io.BytesIO(httpx.get(url).content)).open(
            "donnees_communes.csv"
        ),
        sep=";",
    )
    df = df.sort_values(by="PTOT", ascending=False)
    df = df[:max_number_of_cities]
    df = df.sort_values(by="COM")
    df = df.rename(
        {"COM": "city_code", "Commune": "commune", "Région": "region"}, axis="columns"
    )
    df = df[["city_code", "commune", "region"]]

    return df.to_dict(orient="records")


@task.external_python(
    python=tasks.PYTHON_BIN_PATH,
    retries=15,
)
def extract(city_code: str, commune: str, region: str, to_s3_path: str):
    """Extract the list of creches from monenfant.fr in a given city."""
    import io
    import os

    from airflow.models import Variable
    from airflow.providers.amazon.aws.hooks import s3

    from data_inclusion.pipeline.sources import monenfant

    os.environ["MONENFANT_BASE_URL"] = "https://monenfant.fr"
    os.environ["TWOCAPTCHA_API_KEY"] = Variable.get("TWOCAPTCHA_API_KEY")

    content = monenfant.extract(city_code=city_code, commune=commune, region=region)

    s3_hook = s3.S3Hook(aws_conn_id="s3")
    with io.BytesIO(content) as buf:
        s3_hook.load_file_obj(
            key=to_s3_path,
            file_obj=buf,
            replace=True,
        )


@task.external_python(
    python=tasks.PYTHON_BIN_PATH,
)
def load(schema_name: str, table_name: str, from_s3_path: str):
    import tempfile
    from pathlib import Path

    import pandas as pd

    from airflow.providers.amazon.aws.hooks import s3
    from airflow.providers.postgres.hooks import postgres

    from data_inclusion.pipeline.common import pg
    from data_inclusion.pipeline.sources import monenfant

    s3_hook = s3.S3Hook(aws_conn_id="s3")

    print(f"Using {from_s3_path}")

    with tempfile.TemporaryDirectory() as tmpdir:
        s3_hook = s3.S3Hook(aws_conn_id="s3")
        dfs_list = [
            monenfant.read(Path(s3_hook.download_file(key=p, local_path=tmpdir)))
            for p in s3_hook.list_keys(prefix=from_s3_path)
        ]

    df = pd.concat(dfs_list)

    pg.to_pg(
        hook=postgres.PostgresHook(postgres_conn_id="pg"),
        df=df,
        schema_name=schema_name,
        table_name=table_name,
    )


MAX_NUMBER_OF_CITIES = 2000

# number of concurrent extract tasks
# keep this number low to stay under the radar
EXTRACT_TASK_CONCURRENCY = 2


@dag(
    schedule="@monthly",
    tags=["source"],
    **dags.common_args(use_sentry=True),
)
def import_monenfant():
    source_id = "monenfant"
    stream_id = "creches"

    base_s3_path = s3.get_key(stage="raw", source_id=source_id) / stream_id

    # limit the extraction to the top cities in France
    # because searches on monenfant.fr are limited to a 30km radius around a city
    cities_list = list_cities(max_number_of_cities=MAX_NUMBER_OF_CITIES)

    # map each listed communes to a dedicated extract tasks.
    # given the brittle nature of this extraction (scrap with many calls & captcha),
    # this allows us to leverage airflow fine-grained retries and parallelization
    chain(
        extract.override(
            max_active_tis_per_dag=EXTRACT_TASK_CONCURRENCY,
            map_index_template="{{ task.op_kwargs.city_code }}",
        )
        .partial(to_s3_path=str(base_s3_path / "{{ task.op_kwargs.city_code }}.json"))
        .expand_kwargs(cities_list),
        tasks.create_schema(name=source_id),
        load(
            schema_name=source_id,
            table_name=stream_id,
            from_s3_path=str(base_s3_path),
        ),
    )


import_monenfant()
