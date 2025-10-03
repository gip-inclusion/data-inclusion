WITH contacts AS (
    SELECT DISTINCT RTRIM(UNACCENT(LOWER(courriel)), '.') AS courriel
    FROM {{ ref('int__union_contacts') }}
    WHERE
        source NOT IN ('monenfant', 'reseau-alpha', 'soliguide')
        AND courriel IS NOT NULL
),

brevo_contacts AS (
    SELECT * FROM processings.brevo_import_contacts()
),

final AS (
    SELECT
        contacts.courriel,
        brevo_contacts.has_hardbounced,
        brevo_contacts.was_objected_to
    FROM contacts
    LEFT JOIN brevo_contacts ON contacts.courriel = brevo_contacts.courriel
)

SELECT * FROM final
