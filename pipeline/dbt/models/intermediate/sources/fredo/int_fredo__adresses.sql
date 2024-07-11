WITH structures AS (
    SELECT * FROM {{ ref('stg_fredo__structures') }}
),

final AS (
    SELECT
        id            AS "id",
        commune       AS "commune",
        NULL          AS "code_insee",
        longitude     AS "longitude",
        latitude      AS "latitude",
        _di_source_id AS "source",
        code_postal   AS "code_postal",
        adresse       AS "adresse",
        NULL          AS "complement_adresse"
    FROM structures
)

SELECT * FROM final
