WITH structures AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__structures') }}
),

formations AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__formations') }}
),

structure_adresses AS (
    SELECT
        _di_source_id AS "source",
        id            AS "id",
        NULL          AS "longitude",
        NULL          AS "latitude",
        NULL          AS "complement_adresse",
        NULL          AS "commune",
        NULL          AS "adresse",
        NULL          AS "code_postal",
        NULL          AS "code_insee"
    FROM structures
),

formation_adresses AS (
    SELECT
        _di_source_id AS "source",
        id            AS "id",
        NULL          AS "longitude",
        NULL          AS "latitude",
        NULL          AS "complement_adresse",
        NULL          AS "commune",
        NULL          AS "adresse",
        NULL          AS "code_postal",
        NULL          AS "code_insee"
    FROM formations
),

final AS (
    SELECT * FROM structure_adresses
    UNION ALL
    SELECT * FROM formation_adresses
)

SELECT * FROM final
