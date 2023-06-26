WITH structures AS (
    SELECT * FROM {{ ref('stg_dora__structures') }}
),

services AS (
    SELECT * FROM {{ ref('stg_dora__services') }}
),

structure_adresses AS (
    SELECT
        id                      AS "id",
        longitude               AS "longitude",
        latitude                AS "latitude",
        _di_source_id           AS "source",
        NULLIF(address_2, '')   AS "complement_adresse",
        NULLIF(city, '')        AS "commune",
        NULLIF(address_1, '')   AS "adresse",
        NULLIF(postal_code, '') AS "code_postal",
        NULLIF(city_code, '')   AS "code_insee"
    FROM structures
),

service_adresses AS (
    SELECT
        id                      AS "id",
        longitude               AS "longitude",
        latitude                AS "latitude",
        _di_source_id           AS "source",
        NULLIF(address_2, '')   AS "complement_adresse",
        NULLIF(city, '')        AS "commune",
        NULLIF(address_1, '')   AS "adresse",
        NULLIF(postal_code, '') AS "code_postal",
        NULLIF(city_code, '')   AS "code_insee"
    FROM services
),

final AS (
    SELECT * FROM structure_adresses
    UNION ALL
    SELECT * FROM service_adresses
)

SELECT * FROM final
