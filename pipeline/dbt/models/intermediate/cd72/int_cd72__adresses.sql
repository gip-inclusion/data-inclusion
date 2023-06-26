WITH raw_rows AS (
    SELECT * FROM {{ ref('stg_cd72__rows') }}
),

rows_with_id AS (
    SELECT *
    FROM raw_rows
    WHERE id IS NOT NULL
),

final AS (
    SELECT
        id            AS "id",
        ville         AS "commune",
        code_postal   AS "code_postal",
        NULL          AS "code_insee",
        adresse       AS "adresse",
        NULL          AS "complement_adresse",
        NULL::FLOAT   AS "longitude",
        NULL::FLOAT   AS "latitude",
        _di_source_id AS "source"
    FROM rows_with_id
)

SELECT * FROM final
