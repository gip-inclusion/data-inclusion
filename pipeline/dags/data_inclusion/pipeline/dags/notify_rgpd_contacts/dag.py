from airflow.sdk import dag, task

from data_inclusion.pipeline.common import dags


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
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
                courriels.has_hardbounced IS NOT NULL AS is_known,
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
            GROUP BY 1, 2
        """,
    )

    # new contacts : all those for which we don't even know if they bounced or not
    new_contacts = [
        {"email": courriel, "attributes": {"SERVICES": format_services(noms=noms)}}
        for courriel, is_known, noms in rows
        if not is_known
    ]

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
        contacts=[{"email": courriel} for courriel, _, _ in rows],
    )


@dag(
    description="Sends RGPD notifications to DI users",
    schedule="@monthly",
    **dags.common_args(use_sentry=True),
)
def notify_rgpd_contacts():
    send_rgpd_notice()


notify_rgpd_contacts()
