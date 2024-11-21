WITH structures AS (
    SELECT * FROM {{ ref('stg_fredo__structures') }}
),

final AS (
    SELECT
        id            AS "id",
        commune       AS "commune",
        NULL          AS "code_insee",
        CASE
            WHEN adresse IS NOT NULL THEN latitude
        END           AS "latitude",
        CASE
            WHEN adresse IS NOT NULL THEN longitude
        END           AS "longitude",
        _di_source_id AS "source",
        code_postal   AS "code_postal",
        adresse       AS "adresse",
        NULL          AS "complement_adresse"
    FROM structures
)

SELECT * FROM final
