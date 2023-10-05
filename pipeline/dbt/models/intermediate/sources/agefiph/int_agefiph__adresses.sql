WITH structures AS (
    SELECT * FROM {{ ref('stg_agefiph__structures') }}
),

final AS (
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
)

SELECT * FROM final
