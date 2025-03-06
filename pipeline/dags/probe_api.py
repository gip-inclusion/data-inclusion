import pendulum

from airflow.decorators import dag, task
from airflow.operators import empty

from dag_utils.virtualenvs import PYTHON_BIN_PATH

default_args = {}


@task.external_python(
    python=str(PYTHON_BIN_PATH),
)
def store_probe_results(get_requests: list[str], today: str) -> None:
    import json

    import furl
    import requests

    from airflow.models import Variable

    from dag_utils import s3

    BASE_URI = "https://api.data.inclusion.gouv.fr"
    token_probe = Variable.get("DATA_INCLUSION_API_PROBE_TOKEN")
    headers = {
        "Authorization": f"Bearer {token_probe}",
    }
    for get_request in get_requests:
        url = furl.furl(f"{BASE_URI}/{get_request}")

        response = requests.get(url.add({"size": 100}).url, headers=headers)
        param_slug = [f"{k}-{v}" for k, v in url.query.params.items()]
        file_name = f"tests/{today}/{'-'.join(list(url.path.segments) + param_slug)}"
        s3.store_content(
            f"tests/{today}/{file_name}.json",
            json.dumps(response.json(), indent=2).encode(),
        )


@task.external_python(
    python=str(PYTHON_BIN_PATH),
)
def compare_results(get_requests: list[str], today: str, yesterday: str) -> None:
    import json

    import furl
    from botocore.exceptions import ClientError

    from dag_utils import s3

    error_margin = 0.1
    error = False

    for get_request in get_requests:
        url = furl.furl(get_request)
        param_slug = [f"{k}-{v}" for k, v in url.query.params.items()]
        today_filename = "-".join(list(url.path.segments) + param_slug)
        yesterday_filename = "-".join(list(url.path.segments) + param_slug)
        today_path = f"tests/{today}/{today_filename}"
        yesterday_path = f"tests/{yesterday}/{yesterday_filename}"

        try:
            tmp_filename_yesterday = s3.download_file(yesterday_path)
            yesterday_data = json.load(tmp_filename_yesterday.open())
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                print(f"No data from yesterday for path `{yesterday_path}`")
                continue
            raise e

        tmp_filename_today = s3.download_file(today_path)
        today_dict = json.load(tmp_filename_today.open())

        n_today = today_dict["response"]["total"]
        n_yesterday = yesterday_data["response"]["total"]
        if abs(n_today - n_yesterday) / n_yesterday > error_margin:
            print(f"""
                    For {today_path=},
                    significant difference in the number of items:
                    {n_today=} vs {n_yesterday=}
                """)
            error = True

    if error:
        raise ValueError("Significant difference in the number of items.")

    print("No significant difference in the number of items")


EVERY_DAY_AT_7AM = "0 7 * * *"


@dag(
    dag_id="probe_api",
    description="""
        Probe the API to check if there are no significant
        data differences compared to the last time.
    """,
    start_date=pendulum.datetime(2022, 1, 1),
    default_args=default_args,
    schedule=EVERY_DAY_AT_7AM,
    catchup=False,
)
def import_decoupage_administratif():
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    today = pendulum.now().format("YYYY-MM-DD")
    yesterday = pendulum.now().subtract(days=1).format("YYYY-MM-DD")

    requests = [
        "api/v0/search/services?code_commune=75056&thematiques=apprendre-francais",
        "api/v0/services?code_departement=08",
        "api/v0/structures?code_departement=08",
    ]

    (
        start
        >> store_probe_results(get_requests=requests, today=today)
        >> compare_results(get_requests=requests, today=today, yesterday=yesterday)
        >> end
    )


import_decoupage_administratif()
