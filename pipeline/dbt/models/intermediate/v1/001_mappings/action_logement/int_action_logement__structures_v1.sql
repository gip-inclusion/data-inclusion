WITH structures AS (
    SELECT * FROM {{ ref('stg_action_logement__structures') }}
),

final AS (
    SELECT
        'action-logement'  AS "source",
        'action-logement--' || id AS "id",
        'action-logement--' || id AS "adresse_id",
        -- TODO
    FROM structures
)

SELECT * FROM final
