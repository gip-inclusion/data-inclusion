import os

from sentry_sdk import configure_scope


def fill_sentry_scope(context):
    github_base_url = "https://github.com/gip-inclusion/data-inclusion"

    with configure_scope() as scope:
        dag_id = context.get("dag").dag_id
        task_id = context.get("task_instance").task_id
        task_instance = context.get("task_instance")
        commit_sha = os.getenv("AIRFLOW__SENTRY__RELEASE", None)
        scope.set_tag("dag_id", dag_id)
        scope.set_tag("task_id", task_id)
        scope.set_context(
            "custom",
            {
                "airflow_logs_url": task_instance.log_url,
                "github_commit_url": f"{github_base_url}/commit/{commit_sha}",
            },
        )


def notify_failure_args():
    return {
        "on_failure_callback": fill_sentry_scope,
    }
