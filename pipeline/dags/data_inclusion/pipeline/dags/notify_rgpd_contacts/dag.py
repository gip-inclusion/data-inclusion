from airflow.sdk import dag, task

from data_inclusion.pipeline.common import dags


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
)
def send_rgpd_notice():
    from airflow.providers.postgres.hooks import postgres
    from airflow.sdk import Variable

    from data_inclusion.pipeline.common import brevo

    ALL_CONTACTS_LIST_ID = 6
    CURRENT_CONTACTS_LIST_ID = 5
    TEMPLATE_ID = 56

    brevo_client = brevo.BrevoClient(token=Variable.get("BREVO_API_KEY"))

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")
    contacts = pg_hook.get_records(
        sql="SELECT * FROM public_intermediate.int__courriels_verifies_v1",
    )

    # new contacts : all those for which we don't even know if they bounced or not
    new_emails = [c[0] for c in contacts if c[1] is None]
    brevo_client.empty_list(CURRENT_CONTACTS_LIST_ID)
    brevo_client.import_to_list(CURRENT_CONTACTS_LIST_ID, new_emails)

    brevo_client.create_and_send_email_campaign(
        subject="Vos services d'insertion sur La plateforme de l'inclusion",
        template_id=TEMPLATE_ID,
        to_list_id=CURRENT_CONTACTS_LIST_ID,
        tag="di-rgpd-notice",
        from_email="data-inclusion@inclusion.gouv.fr",
        from_name="La plateforme de l'inclusion",
        reply_to="data-inclusion@inclusion.gouv.fr",
    )

    # only once the campaign is sent : otherwise, if sending fails,
    # contacts would be considered as notified and never be notified.
    all_emails = [c[0] for c in contacts]
    brevo_client.import_to_list(ALL_CONTACTS_LIST_ID, all_emails)


@dag(
    description="Sends RGPD notifications to DI users",
    schedule="@monthly",
    **dags.common_args(use_sentry=True),
)
def notify_rgpd_contacts():
    send_rgpd_notice()


notify_rgpd_contacts()
