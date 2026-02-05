WITH structures AS (
    SELECT * FROM {{ ref('stg_dora__structures') }}
),

services AS (
    SELECT * FROM {{ ref('stg_dora__services') }}
),

structure_adresses AS (
    SELECT
        'dora'             AS "source",
        'dora--' || id     AS "id",
        longitude          AS "longitude",
        latitude           AS "latitude",
        complement_adresse AS "complement_adresse",
        commune            AS "commune",
        adresse            AS "adresse",
        code_postal        AS "code_postal",
        code_insee         AS "code_insee"
    FROM structures
),

service_adresses AS (
    SELECT
        'dora'             AS "source",
        'dora--' || id     AS "id",
        longitude          AS "longitude",
        latitude           AS "latitude",
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

SELECT *
FROM final
WHERE
    (adresse IS NOT NULL OR complement_adresse IS NOT NULL)
    AND (commune IS NOT NULL OR code_insee IS NOT NULL OR code_postal IS NOT NULL)
