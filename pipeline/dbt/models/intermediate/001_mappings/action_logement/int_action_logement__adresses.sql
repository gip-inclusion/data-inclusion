WITH structures AS (
    SELECT * FROM {{ ref('stg_action_logement__structures') }}
),

final AS (
    SELECT
        id                 AS "id",
        longitude          AS "longitude",
        latitude           AS "latitude",
        'action-logement'  AS "source",
        complement_adresse AS "complement_adresse",
        commune            AS "commune",
        adresse            AS "adresse",
        code_postal        AS "code_postal",
        code_insee         AS "code_insee"
    FROM structures
)

SELECT * FROM final
