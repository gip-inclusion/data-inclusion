WITH structures AS (
    SELECT * FROM {{ ref('stg_dora__structures') }}
),

services AS (
    SELECT * FROM {{ ref('stg_dora__services') }}
),

structure_adresses AS (
    SELECT
        id                 AS "id",
        longitude          AS "longitude",
        latitude           AS "latitude",
        _di_source_id      AS "source",
        complement_adresse AS "complement_adresse",
        commune            AS "commune",
        adresse            AS "adresse",
        code_postal        AS "code_postal",
        code_insee         AS "code_insee"
    FROM structures
),

service_adresses AS (
    SELECT
        id                 AS "id",
        longitude          AS "longitude",
        latitude           AS "latitude",
        _di_source_id      AS "source",
        complement_adresse AS "complement_adresse",
        commune            AS "commune",
        adresse            AS "adresse",
        code_postal        AS "code_postal",
        code_insee         AS "code_insee"
    FROM services
),

final AS (
    SELECT * FROM structure_adresses
    UNION ALL
    SELECT * FROM service_adresses
)

SELECT * FROM final
