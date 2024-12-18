WITH structures AS (
    SELECT * FROM {{ ref('stg_imilo__structures') }}
),

final AS (
    SELECT
        id                  AS "id",
        commune             AS "commune",
        code_postal         AS "code_postal",
        code_insee          AS "code_insee",
        adresse             AS "adresse",
        complement_adresse  AS "complement_adresse",
        CAST(NULL AS FLOAT) AS "longitude",
        CAST(NULL AS FLOAT) AS "latitude",
        _di_source_id       AS "source"
    FROM structures
)

SELECT * FROM final
