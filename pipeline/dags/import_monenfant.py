import pendulum

from airflow.decorators import dag, task

from dag_utils import date, notifications
from dag_utils.virtualenvs import PYTHON_BIN_PATH


@task.external_python(
    python=str(PYTHON_BIN_PATH),
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
    python=str(PYTHON_BIN_PATH),
    retries=15,
)
def extract(
    city_code: str,
    commune: str,
    region: str,
    run_id,
    logical_date,
):
    """Extract the list of creches from monenfant.fr in a given city."""
    import os

    from airflow.models import Variable

    from dag_utils import s3
    from dag_utils.sources import monenfant

    os.environ["MONENFANT_BASE_URL"] = Variable.get("MONENFANT_BASE_URL")
    os.environ["TWOCAPTCHA_API_KEY"] = Variable.get("TWOCAPTCHA_API_KEY")

    source_id = "monenfant"
    stream_id = "creches"

    s3_file_path = s3.source_file_path(
        source_id=source_id,
        filename=f"{stream_id}/{city_code}.json",
        run_id=run_id,
        logical_date=logical_date,
    )

    content = monenfant.extract(city_code=city_code, commune=commune, region=region)

    s3.store_content(
        path=s3_file_path,
        content=content,
    )


@task.external_python(
    python=str(PYTHON_BIN_PATH),
)
def load(
    run_id,
    logical_date,
):
    import pandas as pd

    from airflow.providers.amazon.aws.hooks import s3

    from dag_utils import pg, s3 as s3_utils
    from dag_utils.sources import monenfant

    source_id = "monenfant"
    stream_id = "creches"

    s3_file_prefix = s3_utils.source_file_path(
        source_id=source_id,
        filename=stream_id,
        run_id=run_id,
        logical_date=logical_date,
    )

    s3_hook = s3.S3Hook(aws_conn_id="s3")

    dfs_list = (
        monenfant.read(s3_utils.download_file(p))
        for p in s3_hook.list_keys(prefix=s3_file_prefix)
    )

    df = pd.concat(dfs_list)

    df = pd.DataFrame().assign(data=df.apply(lambda row: row.to_dict(), axis="columns"))
    df = df.assign(_di_batch_id=run_id)
    df = df.assign(_di_source_id=source_id)
    df = df.assign(_di_stream_id=stream_id)
    df = df.assign(_di_stream_s3_key=s3_file_prefix)
    df = df.assign(_di_logical_date=logical_date)

    pg.create_schema(source_id)
    pg.load_source_df(source_id=source_id, stream_id=stream_id, df=df)


MAX_NUMBER_OF_CITIES = 2000

# number of concurrent extract tasks
# keep this number low to stay under the radar
EXTRACT_TASK_CONCURRENCY = 2


@dag(
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    default_args=notifications.notify_failure_args(),
    schedule="@monthly",
    catchup=False,
    tags=["source"],
)
def import_monenfant():
    # limit the extraction to the top cities in France
    # because searches on monenfant.fr are limited to a 30km radius around a city
    cities_list = list_cities(max_number_of_cities=MAX_NUMBER_OF_CITIES)

    # map each listed communes to a dedicated extract tasks.
    # given the brittle nature of this extraction (scrap with many calls & captcha),
    # this allows us to leverage airflow fine-grained retries and parallelization
    (
        extract.override(
            max_active_tis_per_dag=EXTRACT_TASK_CONCURRENCY,
            map_index_template="{{ task.op_kwargs.city_code }}",
        ).expand_kwargs(cities_list)
        >> load()
    )


import_monenfant()
