WITH structure_contacts AS (
    SELECT
        email                      AS "courriel",
        'mes-aides:garages:' || id AS contact_uid
    FROM {{ ref('stg_mes_aides__garages') }}
    WHERE email IS NOT NULL
),

service_contacts AS (
    SELECT
        contact_email            AS "courriel",
        'mes-aides:aides:' || id AS contact_uid
    FROM {{ ref('stg_mes_aides__permis_velo') }}
    WHERE contact_email IS NOT NULL
),

final AS (
    SELECT * FROM structure_contacts
    UNION ALL
    SELECT * FROM service_contacts
)

SELECT * FROM final
