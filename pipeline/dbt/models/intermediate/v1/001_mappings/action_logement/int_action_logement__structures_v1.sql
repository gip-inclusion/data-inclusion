WITH structures AS (
    SELECT * FROM {{ ref('stg_action_logement__structures') }}
),

final AS (
    SELECT
    FROM structures
)

SELECT * FROM final
