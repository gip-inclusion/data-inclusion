WITH structure_contacts AS (
    SELECT
        id                         AS "id",
        _di_source_id              AS "source",
        email                      AS "courriel",
        telephone                  AS "telephone",
        NULL                       AS "contact_nom_prenom",
        'mes-aides:garages:' || id AS "contact_uid"
    FROM {{ ref('stg_mes_aides__garages') }}
    WHERE email IS NOT NULL
),

service_contacts AS (
    SELECT
        id                       AS "id",
        _di_source_id            AS "source",
        email                    AS "courriel",
        telephone                AS "telephone",
        NULL                     AS "contact_nom_prenom",
        'mes-aides:aides:' || id AS "contact_uid"
    FROM {{ ref('stg_mes_aides__aides') }}
    WHERE email IS NOT NULL
),

final AS (
    SELECT * FROM structure_contacts
    UNION ALL
    SELECT * FROM service_contacts
)

SELECT * FROM final
