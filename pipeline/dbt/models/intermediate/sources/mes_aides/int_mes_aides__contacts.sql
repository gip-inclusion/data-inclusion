WITH structure_contacts AS (
    SELECT
        id            AS "id",
        _di_source_id AS "source",
        email         AS "courriel",
        telephone     AS "telephone",
        NULL          AS "contact_nom_prenom"
    FROM {{ ref('stg_mes_aides__garages') }}
    WHERE email IS NOT NULL
),

service_contacts AS (
    SELECT
        id            AS "id",
        _di_source_id AS "source",
        contact_email AS "courriel",
        NULL          AS "telephone",
        NULL          AS "contact_nom_prenom"
    FROM {{ ref('stg_mes_aides__permis_velo') }}
    WHERE contact_email IS NOT NULL
),

final AS (
    SELECT * FROM structure_contacts
    UNION ALL
    SELECT * FROM service_contacts
)

SELECT * FROM final
