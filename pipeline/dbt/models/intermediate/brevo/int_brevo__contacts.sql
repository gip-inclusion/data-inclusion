WITH contacts AS (
    SELECT * FROM {{ ref('stg_brevo__contacts') }}
),

final AS (
    SELECT
        id                                 AS "id",
        email                              AS "courriel",
        email_blacklisted                  AS "est_interdit",
        date_di_rgpd_opposition            AS "date_di_rgpd_opposition",
        STRING_TO_ARRAY(contact_uids, ',') AS "contact_uids"
    FROM contacts
)

SELECT * FROM final
