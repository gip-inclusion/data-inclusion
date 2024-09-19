WITH structure_contacts AS (
    SELECT
        id                       AS "id",
        _di_source_id            AS "source",
        courriel                 AS "courriel",
        telephone                AS "telephone",
        NULL                     AS "contact_nom_prenom",
        'dora:structures:' || id AS "contact_uid"
    FROM {{ ref('stg_dora__structures') }}
    WHERE courriel IS NOT NULL
),

service_contacts AS (
    SELECT
        id                     AS "id",
        _di_source_id          AS "source",
        courriel               AS "courriel",
        telephone              AS "telephone",
        contact_nom_prenom     AS "contact_nom_prenom",
        'dora:services:' || id AS "contact_uid"
    FROM {{ ref('stg_dora__services') }}
    WHERE courriel IS NOT NULL
),

final AS (
    SELECT * FROM structure_contacts
    UNION ALL
    SELECT * FROM service_contacts
)

SELECT * FROM final
