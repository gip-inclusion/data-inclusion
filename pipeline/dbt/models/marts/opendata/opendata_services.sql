WITH services AS (
    SELECT * FROM {{ ref('int__validated_services') }}
),

final AS (
    SELECT *
    FROM services
)

SELECT * FROM final
