WITH contacts AS (
    SELECT * FROM {{ ref('stg_brevo__contacts') }}
),

final AS (
    SELECT
        id                                  AS "id",
        email                               AS "courriel",
        email_blacklisted                   AS "has_hardbounced",
        date_di_rgpd_opposition IS NOT NULL AS "was_objected_to"
    FROM contacts
)

SELECT * FROM final
