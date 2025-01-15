import logging

import pendulum

import airflow
from airflow.operators import empty, python

from dag_utils import sentry
from dag_utils.virtualenvs import PYTHON_BIN_PATH

logger = logging.getLogger(__name__)


def _sync_new_contacts_to_brevo():
    from airflow.models import Variable

    from dag_utils import constants, pg
    from dag_utils.sources import brevo

    contacts = pg.hook().get_records(
        sql=(
            """
            SELECT
                json_build_object('email', our_contacts.courriel),
                brevo_contacts.has_hardbounced
            FROM public_intermediate.int__union_contacts AS our_contacts
            LEFT JOIN public_intermediate.int_brevo__contacts AS brevo_contacts
            ON our_contacts.courriel = brevo_contacts.courriel
            WHERE source = ANY (ARRAY['dora', 'mediation_numerique', 'mes-aides'])
            GROUP BY our_contacts.courriel, brevo_contacts.has_hardbounced
            ORDER BY our_contacts.courriel
            """
        )
    )

    # new contacts : all those for which we don't even know if they bounced or not
    new_contacts = [c[0] for c in contacts if c[1] is None]
    all_contacts = [c[0] for c in contacts]

    brevo_client = brevo.BrevoClient(token=Variable.get("BREVO_API_KEY"))

    brevo_client.import_to_list(constants.BREVO_ALL_CONTACTS_LIST_ID, all_contacts)
    brevo_client.empty_list(constants.BREVO_CURRENT_CONTACTS_LIST_ID)
    brevo_client.import_to_list(constants.BREVO_CURRENT_CONTACTS_LIST_ID, new_contacts)


def _send_rgpd_notice():
    from airflow.models import Variable

    from dag_utils import constants
    from dag_utils.sources import brevo

    brevo_client = brevo.BrevoClient(token=Variable.get("BREVO_API_KEY"))
    brevo_client.create_and_send_email_campaign(
        subject="[data·inclusion] Notification RGPD",
        template_id=2,
        to_list_id=constants.BREVO_CURRENT_CONTACTS_LIST_ID,
        tag="di-rgpd-notice",
        from_email="bonjour@data.inclusion.gouv.fr",
        from_name="L'équipe data inclusion",
        reply_to="ne-pas-repondre@data.inclusion.gouv.fr",
    )


def _wait_for_brevo():
    import time

    time.sleep(60)


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

    sync_new_contacts_to_brevo = python.ExternalPythonOperator(
        task_id="sync_new_contacts_to_brevo",
        python=str(PYTHON_BIN_PATH),
        python_callable=_sync_new_contacts_to_brevo,
    )

    # It seems that after syncing the contact lists, Brevo needs some time
    # to actually understand that the new mailing list is full. Has it been
    # turned to async behind the scenes recently ? Probable. We could also
    # poll until the list returns the expected number of contacts, but...
    brevo_delay = python.ExternalPythonOperator(
        task_id="brevo_delay",
        python=str(PYTHON_BIN_PATH),
        python_callable=_wait_for_brevo,
    )

    send_rgpd_notice = python.ExternalPythonOperator(
        task_id="send_rgpd_notice",
        python=str(PYTHON_BIN_PATH),
        python_callable=_send_rgpd_notice,
    )

    (start >> sync_new_contacts_to_brevo >> brevo_delay >> send_rgpd_notice >> end)
