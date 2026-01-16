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

-- services may not have their own address (they use the parent structure's address)
-- only include services that actually have their own address data
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
    WHERE
        (adresse IS NOT NULL OR complement_adresse IS NOT NULL)
        AND (commune IS NOT NULL OR code_insee IS NOT NULL OR code_postal IS NOT NULL)
),

final AS (
    SELECT * FROM structure_adresses
    UNION ALL
    SELECT * FROM service_adresses
)

SELECT * FROM final
