WITH structures AS (
    SELECT * FROM {{ ref('int__enhanced_structures') }}
),

final AS (
    SELECT
        source,
        COUNT(*) AS "count"
    FROM structures
    GROUP BY 1
    ORDER BY 2 DESC
)

SELECT * FROM final
