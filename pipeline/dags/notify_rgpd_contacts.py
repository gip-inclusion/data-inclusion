import logging

import pendulum

import airflow
from airflow.operators import empty, python

from dag_utils.virtualenvs import PYTHON_BIN_PATH

default_args = {}

logger = logging.getLogger(__name__)


def _sync_new_contacts_to_brevo():
    from collections import defaultdict

    from airflow.models import Variable

    from dag_utils import constants, pg
    from dag_utils.sources import brevo

    dora_contacts = pg.hook().get_records(
        sql=(
            "SELECT courriel, ARRAY_AGG(contact_uid) as contact_uids "
            "FROM public_intermediate.int_dora__contacts "
            "GROUP BY courriel"
        )
    )

    brevo_contacts = pg.hook().get_records(
        sql=(
            "SELECT courriel, contact_uids "
            "FROM public_intermediate.int_brevo__contacts"
        )
    )

    brevo_contacts_uid_set = {
        uid for _, contact_uids in brevo_contacts for uid in contact_uids
    }
    brevo_contacts_map = {email: contact_uids for email, contact_uids in brevo_contacts}

    # We only consider a contact as "new" if BOTH its contact uid AND its email are new.
    # If the email is not new but linked to a new contact UID, update the associated
    # list accordingly.
    new_contacts_map = defaultdict(list)
    for email, contact_uids in dora_contacts:
        for contact_uid in contact_uids:
            if contact_uid not in brevo_contacts_uid_set:
                if email in brevo_contacts_map:
                    brevo_contacts_map[email].append(contact_uid)
                else:
                    new_contacts_map[email].append(contact_uid)

    new_contacts = [
        {"email": email, "attributes": {"contact_uids": ",".join(contact_uids)}}
        for email, contact_uids in new_contacts_map.items()
    ]
    all_contacts = new_contacts + [
        {"email": email, "attributes": {"contact_uids": ",".join(contact_uids)}}
        for email, contact_uids in brevo_contacts_map.items()
    ]

    brevo_client = brevo.BrevoClient(token=Variable.get("BREVO_API_KEY"))

    brevo_client.import_to_list(constants.BREVO_ALL_CONTACTS_LIST_ID, all_contacts)
    brevo_client.empty_list(constants.BREVO_CURRENT_CONTACTS_LIST_ID)
    brevo_client.import_to_list(constants.BREVO_CURRENT_CONTACTS_LIST_ID, new_contacts)


def _send_rgpd_notice():
    from airflow.models import Variable

    from data_inclusion.scripts.tasks import brevo

    from dags.dag_utils import constants

    brevo_client = brevo.BrevoClient(token=Variable.get("BREVO_API_KEY"))
    brevo_client.create_and_send_email_campaign(
        subject="[data·inclusion] Notification RGPD",
        template_id=2,
        to_list_id=constants.BREVO_CURRENT_CONTACTS_LIST_ID,
        tag="di-rgpd-notice",
        from_email="bonjour@data.inclusion.beta.gouv.fr",
        from_name="L'équipe data inclusion",
        reply_to="ne-pas-repondre@data.inclusion.beta.gouv.fr",
    )


with airflow.DAG(
    dag_id="notify_rgpd_contacts",
    description="Sends RGPD notifications to DI users",
    start_date=pendulum.datetime(2023, 11, 1),
    default_args=default_args,
    schedule="@monthly",
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    sync_new_contacts_to_brevo = python.ExternalPythonOperator(
        task_id="sync_new_contacts_to_brevo",
        python=str(PYTHON_BIN_PATH),
        python_callable=_sync_new_contacts_to_brevo,
    )

    send_rgpd_notice = python.ExternalPythonOperator(
        task_id="send_rgpd_notice",
        python=str(PYTHON_BIN_PATH),
        python_callable=_send_rgpd_notice,
    )

    (start >> sync_new_contacts_to_brevo >> send_rgpd_notice >> end)
