WITH organisations AS (
    SELECT * FROM {{ ref('stg_cd35__organisations') }}
),

final AS (
    SELECT
        id                 AS "id",
        commune            AS "commune",
        code_postal        AS "code_postal",
        code_insee         AS "code_insee",
        adresse            AS "adresse",
        complement_adresse AS "complement_adresse",
        longitude          AS "longitude",
        latitude           AS "latitude",
        _di_source_id      AS "source"
    FROM organisations
)

SELECT * FROM final
