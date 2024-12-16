import logging

from airflow import exceptions
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.context import Context

from dag_utils import date

logger = logging.getLogger(__file__)


def notify_webhook(context: Context, conn_id: str):
    """Format context and notify the webhook identify by the given connection id"""

    dag = context["dag"]
    task = context["task"]
    task_instance = context["task_instance"]

    # if AIRFLOW_CONN_<conn_id> not set, do nothing
    try:
        http_hook = HttpHook(method="POST", http_conn_id=conn_id)
        http_hook.run(
            json={
                "context": {
                    "dag": {"dag_id": dag.dag_id},
                    "task": {"task_id": task.task_id},
                    "task_instance": {
                        "state": task_instance.state,
                        "log_url": task_instance.log_url,
                        "start_date": date.local_day_datetime_str(
                            task_instance.start_date
                        ),
                    },
                    "logical_date": date.local_day_datetime_str(
                        context["logical_date"]
                    ),
                }
            }
        )
    except exceptions.AirflowNotFoundException:
        logger.warning("Webhook notifier disabled.")


# FIXME(vmttn) : This could be a DAG factory instead
def notify_failure_args():
    return {
        "on_failure_callback": lambda context: notify_webhook(context, "mattermost")
    }
