import logging

import pendulum

import airflow
from airflow.operators import empty, python

from dag_utils import sentry
from dag_utils.virtualenvs import PYTHON_BIN_PATH

logger = logging.getLogger(__name__)


def _send_rgpd_notice():
    import time

    from airflow.models import Variable

    from dag_utils import pg
    from dag_utils.sources import brevo

    ALL_CONTACTS_LIST_ID = 6
    CURRENT_CONTACTS_LIST_ID = 5

    brevo_client = brevo.BrevoClient(token=Variable.get("BREVO_API_KEY"))

    contacts = pg.hook().get_records(
        sql="SELECT * FROM public_intermediate.int__courriels_verifies",
    )

    all_emails = [c[0] for c in contacts]
    brevo_client.import_to_list(ALL_CONTACTS_LIST_ID, all_emails)

    # new contacts : all those for which we don't even know if they bounced or not
    new_emails = [c[0] for c in contacts if c[1] is None]
    brevo_client.empty_list(CURRENT_CONTACTS_LIST_ID)
    brevo_client.import_to_list(CURRENT_CONTACTS_LIST_ID, new_emails)

    # sleep a bit: be sure that the new contacts list is finalized.
    time.sleep(60)

    brevo_client.create_and_send_email_campaign(
        subject="[data·inclusion] Notification RGPD",
        template_id=2,
        to_list_id=CURRENT_CONTACTS_LIST_ID,
        tag="di-rgpd-notice",
        from_email="bonjour@data.inclusion.gouv.fr",
        from_name="L'équipe data inclusion",
        reply_to="ne-pas-repondre@data.inclusion.gouv.fr",
    )


with airflow.DAG(
    dag_id="notify_rgpd_contacts",
    description="Sends RGPD notifications to DI users",
    start_date=pendulum.datetime(2023, 11, 1),
    default_args=sentry.notify_failure_args(),
    schedule="@monthly",
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    send_rgpd_notice = python.ExternalPythonOperator(
        task_id="send_rgpd_notice",
        python=str(PYTHON_BIN_PATH),
        python_callable=_send_rgpd_notice,
    )

    (start >> send_rgpd_notice >> end)
