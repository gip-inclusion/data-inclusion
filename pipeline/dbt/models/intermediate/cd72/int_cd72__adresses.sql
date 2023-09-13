WITH structures AS (
    SELECT * FROM {{ ref('stg_cd72__structures') }}
),

services AS (
    SELECT * FROM {{ ref('stg_cd72__services') }}
),

structure_adresses AS (
    SELECT
        id                  AS "id",
        commune             AS "commune",
        code_postal         AS "code_postal",
        NULL                AS "code_insee",
        adresse             AS "adresse",
        NULL                AS "complement_adresse",
        _di_source_id       AS "source",
        CAST(NULL AS FLOAT) AS "longitude",
        CAST(NULL AS FLOAT) AS "latitude"
    FROM structures
),

service_adresses AS (
    SELECT
        id                  AS "id",
        commune             AS "commune",
        code_postal         AS "code_postal",
        NULL                AS "code_insee",
        adresse             AS "adresse",
        NULL                AS "complement_adresse",
        _di_source_id       AS "source",
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
