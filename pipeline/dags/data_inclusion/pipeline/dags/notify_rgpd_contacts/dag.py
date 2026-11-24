from airflow.sdk import dag, task

from data_inclusion.pipeline.common import dags


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
    # never retry : the campaign could be sent twice
    retries=0,
)
def send_rgpd_notice():
    import html

    from airflow.providers.postgres.hooks import postgres
    from airflow.sdk import Variable

    from data_inclusion.pipeline.common import brevo

    ALL_CONTACTS_LIST_ID = 6
    CURRENT_CONTACTS_LIST_ID = 5
    TEMPLATE_ID = 56
    MAX_SERVICES = 10

    def format_services(noms: list[str]) -> str:
        items = [f"<li>{html.escape(nom)}</li>" for nom in noms[:MAX_SERVICES]]
        if len(noms) > MAX_SERVICES:
            items.append(f"<li>et {len(noms) - MAX_SERVICES} autres services</li>")
        return f"<ul>{''.join(items)}</ul>" if items else ""

    brevo_client = brevo.BrevoClient(token=Variable.get("BREVO_API_KEY"))

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")
    rows = pg_hook.get_records(
        sql="""
            SELECT
                courriels.courriel,
                ARRAY_REMOVE(
                    ARRAY_AGG(DISTINCT services.nom ORDER BY services.nom),
                    NULL
                ) AS services
            FROM public_intermediate.int__courriels_verifies_v1 AS courriels
            LEFT JOIN public_intermediate.int__union_contacts_v1 AS contacts
                ON
                    RTRIM(UNACCENT(LOWER(contacts.courriel)), '.') = courriels.courriel
                    -- same exclusions as int__courriels_verifies_v1
                    AND contacts.source NOT IN (
                        'monenfant', 'reseau-alpha', 'soliguide'
                    )
            LEFT JOIN public_marts.marts__services_v1 AS services
                ON
                    contacts.source = services.source
                    AND contacts.id = services.id
                    AND services._is_valid
            WHERE NOT COALESCE(courriels.was_objected_to, FALSE)
            GROUP BY 1
        """,
    )

    # rely on brevo rather than on the datawarehouse, which can be outdated
    notified_emails = {
        contact["email"]
        for contact in brevo_client.list_contacts(list_id=ALL_CONTACTS_LIST_ID)
    }
    new_contacts = [
        {"email": courriel, "attributes": {"SERVICES": format_services(noms=noms)}}
        for courriel, noms in rows
        if courriel and courriel not in notified_emails
    ]
    if len(new_contacts) == 0:
        return

    brevo_client.empty_list(list_id=CURRENT_CONTACTS_LIST_ID)
    brevo_client.import_to_list(
        list_id=CURRENT_CONTACTS_LIST_ID,
        contacts=new_contacts,
    )

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
    brevo_client.import_to_list(
        list_id=ALL_CONTACTS_LIST_ID,
        contacts=[{"email": contact["email"]} for contact in new_contacts],
        # the campaign is sent : a timeout must not fail the task and invite a
        # manual rerun, which would notify contacts again.
        wait=False,
    )


@dag(
    description="Sends RGPD notifications to DI users",
    schedule="@monthly",
    **dags.common_args(use_sentry=True),
)
def notify_rgpd_contacts():
    send_rgpd_notice()


notify_rgpd_contacts()
