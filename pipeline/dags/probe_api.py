import pendulum

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

from dag_utils.virtualenvs import PYTHON_BIN_PATH


@task.external_python(
    python=str(PYTHON_BIN_PATH),
)
def store_probe_results(get_requests: list[str], today: str) -> None:
    import io
    import json

    import furl
    import requests

    from airflow.models import Variable
    from airflow.providers.amazon.aws.hooks import s3

    from dag_utils.sources.utils import filename_from_url

    BASE_URI = "https://api.data.inclusion.gouv.fr"
    token_probe = Variable.get("DATA_INCLUSION_API_PROBE_TOKEN")
    headers = {
        "Authorization": f"Bearer {token_probe}",
    }
    s3_hook = s3.S3Hook(aws_conn_id="s3")

    for get_request in get_requests:
        url = furl.furl(f"{BASE_URI}/{get_request}")
        filename = filename_from_url(list(url.path.segments), url.query.params)
        # For size, no need to get too many results.
        # We only want to compare the total number of items
        response = requests.get(url.add({"size": 100}).url, headers=headers)
        response.raise_for_status()
        today_path = f"tests/{today}/{filename}"

        content = json.dumps(response.json(), indent=2).encode()
        with io.BytesIO(content) as buf:
            s3_hook.load_file_obj(
                key=today_path,
                file_obj=buf,
                replace=True,
            )


@task.external_python(
    python=str(PYTHON_BIN_PATH),
)
def compare_results(get_requests: list[str], today: str, yesterday: str) -> None:
    import json

    import furl
    from botocore.exceptions import ClientError

    from airflow.providers.amazon.aws.hooks import s3

    from dag_utils.sources.utils import filename_from_url

    s3_hook = s3.S3Hook(aws_conn_id="s3")

    # 10% like the threshold for source stats alerts. Adjust if needed.
    error_margin = 0.1
    error = False

    for get_request in get_requests:
        url = furl.furl(get_request)
        filename = filename_from_url(list(url.path.segments), url.query.params)
        today_path = f"tests/{today}/{filename}"
        yesterday_path = f"tests/{yesterday}/{filename}"

        try:
            with open(s3_hook.download_file(key=yesterday_path)) as fp:
                yesterday_data = json.load(fp)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                print(f"No data from yesterday for path `{yesterday_path}`")
                continue
            raise e

        with open(s3_hook.download_file(key=today_path)) as fp:
            today_dict = json.load(fp)

        n_today = today_dict["total"]
        n_yesterday = yesterday_data["total"]
        if abs(n_today - n_yesterday) / n_yesterday > error_margin:
            print(f"""
                    For {today_path=},
                    significant difference in the number of items:
                    {n_today=} vs {n_yesterday=}
                """)
            error = True
        else:
            print(f"No significant difference for {today_path=}")

    if error:
        raise ValueError("Significant difference in the number of items.")


EVERY_DAY_AT_7AM = "0 7 * * *"


@dag(
    dag_id="probe_api",
    description="""
        Probe the API to check if there are no significant
        data differences compared to the last time.
    """,
    start_date=pendulum.datetime(2022, 1, 1),
    schedule=EVERY_DAY_AT_7AM,
    catchup=False,
)
def import_decoupage_administratif():
    today = pendulum.now().format("YYYY-MM-DD")
    yesterday = pendulum.now().subtract(days=1).format("YYYY-MM-DD")

    requests = [
        "api/v0/search/services?code_commune=75056&thematiques=apprendre-francais",
        "api/v0/services?code_departement=08",
        "api/v0/structures?code_departement=08",
    ]

    chain(
        store_probe_results(get_requests=requests, today=today),
        compare_results(get_requests=requests, today=today, yesterday=yesterday),
    )


import_decoupage_administratif()
