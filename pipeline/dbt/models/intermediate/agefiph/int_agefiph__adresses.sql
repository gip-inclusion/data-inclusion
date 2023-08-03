WITH structures AS (
    SELECT * FROM {{ ref('stg_agefiph__structures') }}
),

services AS (
    SELECT * FROM {{ ref('stg_agefiph__services') }}
),

structure_adresses AS (
    SELECT
        _di_source_id       AS "source",
        id                  AS "id",
        commune             AS "commune",
        code_postal         AS "code_postal",
        code_insee          AS "code_insee",
        adresse             AS "adresse",
        complement_adresse  AS "complement_adresse",
        CAST(NULL AS FLOAT) AS "longitude",
        CAST(NULL AS FLOAT) AS "latitude"
    FROM structures
),

service_adresses AS (
    SELECT
        _di_source_id       AS "source",
        id                  AS "id",
        commune             AS "commune",
        code_postal         AS "code_postal",
        code_insee          AS "code_insee",
        adresse             AS "adresse",
        complement_adresse  AS "complement_adresse",
        CAST(NULL AS FLOAT) AS "longitude",
        CAST(NULL AS FLOAT) AS "latitude"
    FROM services
),

final AS (
    SELECT * FROM structure_adresses
    UNION ALL
    SELECT * FROM service_adresses
)

SELECT * FROM final
