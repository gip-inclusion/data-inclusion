{{
    config(
        materialized="incremental",
        unique_key="courriel",
    )
}}

WITH contacts AS (
    SELECT DISTINCT courriel FROM {{ ref('int__union_contacts') }}
    WHERE source NOT IN ('mon-enfant', 'reseau-alpha', 'soliguide')
),

brevo_contacts AS (
    SELECT * FROM processings.sync_emails(
        (
            SELECT JSONB_AGG(contacts.courriel)
            FROM contacts
            {% if is_incremental() %}
                LEFT JOIN {{ this }} ON contacts.courriel = {{ this }}.courriel
                WHERE {{ this }}.courriel IS NULL
            {% endif %}
        ),
        {{ var('all_contacts_list_id', 6) }},
        {{ var('current_contacts_list_id', 0) }}
    )
),

final AS (
    SELECT * FROM brevo_contacts
    -- This is_incremental() block is only necessary when BREVO_API_KEY is not set,
    -- to avoid emptying the table completely in that case.
    -- If BREVO_API_KEY is set, the entire output of processings.sync_emails() would be stored.
    {% if is_incremental() %}
        UNION ALL
        SELECT * FROM {{ this }}
        WHERE NOT EXISTS (
            SELECT
            FROM brevo_contacts
            WHERE brevo_contacts.courriel = {{ this }}.courriel
        )
    {% endif %}
)

SELECT * FROM final
