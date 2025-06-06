WITH structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

final AS (
    SELECT
        id            AS "id",
        commune       AS "commune",
        code_postal   AS "code_postal",
        code_insee    AS "code_insee",
        adresse       AS "adresse",
        NULL          AS "complement_adresse",
        longitude     AS "longitude",
        latitude      AS "latitude",
        _di_source_id AS "source"
    FROM structures
)

SELECT * FROM final
