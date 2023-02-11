WITH structures AS (
    SELECT * FROM {{ ref('int__validated_structures') }}
),

final AS (
    SELECT
        source,
        COUNT(*)
    FROM structures
    GROUP BY 1
    ORDER BY 2 DESC
)

SELECT * FROM final
