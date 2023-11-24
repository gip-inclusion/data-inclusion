WITH structure_contacts AS (
    SELECT
        -- For an unknown reason, some emails are polluted by soft hyphen characters
        REPLACE(courriel, U&'\00ad', '') AS "courriel",
        'dora:structures:' || id         AS contact_uid
    FROM {{ ref('stg_dora__structures') }}
    WHERE courriel IS NOT NULL
),

service_contacts AS (
    SELECT
        -- For an unknown reason, some emails are polluted by soft hyphen characters
        REPLACE(courriel, U&'\00ad', '') AS "courriel",
        'dora:services:' || id           AS contact_uid
    FROM {{ ref('stg_dora__services') }}
    WHERE courriel IS NOT NULL
),

final AS (
    SELECT * FROM structure_contacts
    UNION ALL
    SELECT * FROM service_contacts
)

SELECT * FROM final
