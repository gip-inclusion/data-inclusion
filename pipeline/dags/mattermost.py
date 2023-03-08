import json
import os
import urllib

from airflow.models import Variable

# adapted from https://github.com/MTES-MCT/trackdechets-airflow-workspace/blob/main/dags/mattermost.py 
def mm_failed_task(context):
    """
    Function to be used as a callable for on_failure_callback.
    Send a Mattermost alert.
    """

    mm_webhook_url = Variable.get("MATTERMOST_WEBHOOK")
    base_url = Variable.get("AIRFLOW_BASE_URL")
    if mm_webhook_url is None:
        return

    # Set all of the contextual vars
    execution_date = context["ts"]
    dag_context = context["dag"]
    dag_name = dag_context.dag_id
    dag_id = context["dag"].dag_id
    task_name = context["task"].task_id
    task_id = context["task_instance"].task_id
    execution_date_pretty = context["execution_date"].strftime(
        "%a, %b %d, %Y at %-I:%M %p UTC"
    )

    # Generate the link to the logs
    log_params = urllib.parse.urlencode(
        {"dag_id": dag_id, "task_id": task_id, "execution_date": execution_date}
    )
    log_link = f"{base_url}/log?{log_params}"
    log_link_markdown = f"[View Logs]({log_link})"

    body = f"""**Error during DAG run:**\n\n| DAG | Task | Logs | Timestamp |
        |-----|------|------|-----------|
        |{dag_name}|{task_name}|{log_link_markdown}|{execution_date_pretty}|"""

    payload = {"username": "airflow", "text": body}

    os.system(
        f"""curl -i -X POST {mm_webhook_url} -H 'Content-Type: application/json' \
        --data-binary @- <<'EOF'
            {json.dumps(payload)}
        EOF
    """
    )