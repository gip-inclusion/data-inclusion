WITH structures AS (
    SELECT * FROM {{ ref('stg_action_logement__structures') }}
),

final AS (
    SELECT
        'action-logement'         AS "source",
        'action-logement--' || id AS "id",
        CAST(NULL AS FLOAT)       AS "longitude",
        CAST(NULL AS FLOAT)       AS "latitude",
        complement_adresse        AS "complement_adresse",
        commune                   AS "commune",
        adresse                   AS "adresse",
        code_postal               AS "code_postal",
        NULL                      AS "code_insee"
    FROM structures
)

SELECT * FROM final
