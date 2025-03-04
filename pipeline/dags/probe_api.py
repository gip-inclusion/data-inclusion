import pendulum

import airflow
from airflow.operators import empty, python
from airflow.utils.task_group import TaskGroup

from dag_utils.virtualenvs import PYTHON_BIN_PATH

default_args = {}


def _store_probe_results(route: str, request_params: dict, s3_path: str) -> None:
    import json

    import requests

    from airflow.models import Variable

    from dag_utils import s3

    BASE_URI = "api.data.inclusion.gouv.fr"
    token_probe = Variable.get("DATA_INCLUSION_API_PROBE_TOKEN")

    if token_probe == "":
        raise ValueError("DATA_INCLUSION_API_PROBE_TOKEN is not set")

    headers = {
        "Authorization": f"Bearer {token_probe}",
    }
    request_params["size"] = 100

    response = requests.get(
        f"https://{BASE_URI}/{route}", params=request_params, headers=headers
    )
    dump_request = {
        "url": response.url,
        "params": request_params,
        "response": response.json(),
    }

    s3.store_content(
        s3_path,
        json.dumps(dump_request, indent=2).encode("utf-8"),
    )


def _compare_results(today_path: str, yesterday_path: str, error_margin: float) -> None:
    from botocore.exceptions import ClientError

    from dag_utils import s3
    from dag_utils.sources import utils

    class ProbeError(Exception):
        def __init__(self, n_today, n_yesterday):
            self.n_today = n_today
            self.n_yesterday = n_yesterday

        def __str__(self):
            return f"""
                Significant difference in the number of items:
                {self.n_today=} vs {self.n_yesterday=}
            """

    try:
        tmp_filename_yesterday = s3.download_file(yesterday_path)
        yesterday_data = utils.read_dict(tmp_filename_yesterday)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f"No data from yesterday for path `{yesterday_path}`")
            return
        raise e

    tmp_filename_today = s3.download_file(today_path)
    today_dict = utils.read_dict(tmp_filename_today)

    n_today = today_dict["response"]["total"]
    n_yesterday = yesterday_data["response"]["total"]
    if abs(n_today - n_yesterday) / n_yesterday > error_margin:
        raise ProbeError(n_today, n_yesterday)

    print("No significant difference in the number of items")


EVERY_DAY_AT_7AM = "0 7 * * *"

with airflow.DAG(
    dag_id="probe_api",
    description="""
        Probe the API to check if there are no significant
        data differences compared to the last time.
    """,
    start_date=pendulum.datetime(2022, 1, 1),
    default_args=default_args,
    schedule=EVERY_DAY_AT_7AM,
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    today = pendulum.now().format("YYYY-MM-DD")
    yesterday = pendulum.now().subtract(days=1).format("YYYY-MM-DD")

    requests = [
        {
            "slug": "recherche-services-apprendre-francais-75056",
            "route": "api/v0/search/services",
            "request_params": {
                "code_commune": "75056",
                "thematiques": "apprendre-francais",
            },
        },
        {
            "slug": "liste-services-departement-08",
            "route": "api/v0/services",
            "request_params": {
                "code_departement": "08",
            },
        },
        {
            "slug": "liste-structures-departement-08",
            "route": "api/v0/structures",
            "request_params": {
                "code_departement": "08",
            },
        },
    ]

    for request in requests:
        today_path = f"tests/api/{today}/{request['slug']}.json"
        with TaskGroup(group_id=request["slug"]) as source_task_group:
            get_api_requests = python.ExternalPythonOperator(
                task_id="get_api_requests",
                python=str(PYTHON_BIN_PATH),
                python_callable=_store_probe_results,
                op_kwargs={
                    "route": request["route"],
                    "request_params": request["request_params"],
                    "s3_path": today_path,
                },
            )

            compare_results = python.ExternalPythonOperator(
                task_id="compare_results",
                python=str(PYTHON_BIN_PATH),
                python_callable=_compare_results,
                op_kwargs={
                    "today_path": today_path,
                    "yesterday_path": f"tests/api/{yesterday}/{request['slug']}.json",
                    "error_margin": 0.1,
                },
            )

            start >> get_api_requests >> compare_results >> end
