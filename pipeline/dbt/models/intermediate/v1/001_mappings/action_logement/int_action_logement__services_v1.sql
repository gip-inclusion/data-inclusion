WITH services AS (
    SELECT * FROM {{ ref('stg_action_logement__services') }}
),

structures AS (
    SELECT * FROM {{ ref('int_action_logement__structures') }}
),

final AS (
    SELECT
    FROM services
    CROSS JOIN structures
)

SELECT * FROM final
