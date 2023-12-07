import logging
import textwrap

from airflow import exceptions
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.context import Context

from dag_utils import date

logger = logging.getLogger(__file__)


def format_failure(context: Context) -> str:
    dag = context["dag"]
    task_name = context["task"].task_id
    task_instance = context["task_instance"]
    logical_date_ds = date.local_day_datetime_str(context["logical_date"])
    start_date_ds = date.local_day_datetime_str(task_instance.start_date)

    # AIRFLOW__WEBSERVER__BASE_URL should be set to the public url to be able
    # to access logs

    return textwrap.dedent(
        f"""\
        | Status    | DAG          | Task        | Logs                          | Logical date      | Start date      |
        | --------- | ------------ | ----------- | ----------------------------- | ----------------- | --------------- |
        | Failed 🔴 | {dag.dag_id} | {task_name} | [📜]({task_instance.log_url}) | {logical_date_ds} | {start_date_ds} |
        """  # noqa: E501
    )


def notify_webhook(context: Context, conn_id: str, format_fn):
    """Format context and notify the webhook identify by the given connection id"""

    # if AIRFLOW_CONN_<conn_id> not set, do nothing

    try:
        http_hook = HttpHook(http_conn_id=conn_id)
        http_hook.run(json={"text": format_fn(context)})
    except exceptions.AirflowNotFoundException:
        logger.warning("Webhook notifier disabled.")
        return
