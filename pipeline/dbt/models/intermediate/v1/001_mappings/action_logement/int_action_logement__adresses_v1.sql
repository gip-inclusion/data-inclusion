WITH structures AS (
    SELECT * FROM {{ ref('stg_action_logement__structures_v1') }}
),

final AS (
    SELECT
        'action-logement'  AS "source",
        'action-logement--' || id AS "id",
        longitude          AS "longitude",
        latitude           AS "latitude",
        complement_adresse AS "complement_adresse",
        commune            AS "commune",
        adresse            AS "adresse",
        code_postal        AS "code_postal",
        NULL         AS "code_insee"
    FROM structures
)

SELECT * FROM final
