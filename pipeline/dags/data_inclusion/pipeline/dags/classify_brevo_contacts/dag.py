from airflow.sdk import Variable, dag, task

from data_inclusion.pipeline.common import dags


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
)
def classify_blocklisted_contacts():
    from data_inclusion.pipeline.common import brevo

    ALL_CONTACTS_LIST_ID = 6

    brevo_client = brevo.BrevoClient(token=Variable.get("BREVO_API_KEY"))
    brevo_client.classify_blocklisted_contacts(list_id=ALL_CONTACTS_LIST_ID)


@dag(
    description="Stores why brevo contacts are blocklisted",
    schedule="@daily",
    **dags.common_args(use_sentry=True),
)
def classify_brevo_contacts():
    classify_blocklisted_contacts()


classify_brevo_contacts()
