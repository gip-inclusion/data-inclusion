import logging
import textwrap

import pendulum
from airflow import exceptions
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.context import Context

logger = logging.getLogger(__file__)


def format_failure(context: Context) -> str:
    dag = context["dag"]
    task_name = context["task"].task_id
    task_instance = context["task_instance"]
    logical_date_ds = pendulum.instance(
        context["logical_date"].astimezone(pendulum.timezone("Europe/Paris"))
    ).to_day_datetime_string()
    start_date_ds = pendulum.instance(
        task_instance.start_date.astimezone(pendulum.timezone("Europe/Paris"))
    ).to_day_datetime_string()

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
    except exceptions.AirflowNotFoundException:
        logger.warning("Webhook notifier disabled.")
        return

    http_hook.run(json={"text": format_fn(context)})
