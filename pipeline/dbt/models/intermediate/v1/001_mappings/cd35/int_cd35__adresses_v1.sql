WITH organisations AS (
    SELECT * FROM {{ ref('stg_cd35__organisations') }}
),

final AS (
    SELECT
        'cd35'             AS "source",
        'cd35--' || id     AS "id",
        longitude          AS "longitude",
        latitude           AS "latitude",
        complement_adresse AS "complement_adresse",
        commune            AS "commune",
        adresse            AS "adresse",
        code_postal        AS "code_postal",
        code_insee         AS "code_insee"
    FROM organisations
)

SELECT * FROM final
