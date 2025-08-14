import os

from sentry_sdk import configure_scope


def before_send(event, hint):
    if (
        "exception" in event
        and "values" in event["exception"]
        and event["exception"]["values"]
    ):
        exception_str = (
            # Airflow wraps any exception under `AirflowException`, we want to get the
            # original one. Thankfully the exception string is formatted as such:
            # 'value': 'Process [...]\nname "SomeException" is not defined'
            # We want to keep the last line of the exception string (the original exc
            # object is not directly accessible unfortunately)
            event["exception"]["values"][0].get("value", "exception").split("\n")[-1]
        )
        dag = event["tags"].get("dag_id")
        task = event["tags"].get("task_id")
        if dag and task:
            exception_str = f"{dag=} {task=} : {exception_str}"
        event["exception"]["values"][0]["type"] = exception_str
    return event


def fill_sentry_scope(context):
    github_base_url = "https://github.com/gip-inclusion/data-inclusion"

    with configure_scope() as scope:
        dag_id = context.get("dag").dag_id
        task_id = context.get("task_instance").task_id
        task_instance = context.get("task_instance")
        commit_sha = os.getenv("AIRFLOW__SENTRY__RELEASE", None)
        scope.set_tag("dag_id", dag_id)
        scope.set_tag("task_id", task_id)
        # Fine-tune the fingerprint to force de-grouping issues if not from
        # the same DAG
        scope.fingerprint = ["{{ default }}", dag_id]
        scope.set_context(
            "custom",
            {
                "airflow_logs_url": task_instance.log_url,
                "github_commit_url": f"{github_base_url}/commit/{commit_sha}",
            },
        )
