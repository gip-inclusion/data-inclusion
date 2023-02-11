WITH structures AS (
    SELECT * FROM {{ ref('int__enhanced_structures') }}
),

final AS (
    SELECT
        source,
        COUNT(*) FILTER (WHERE has_deprecated_siret)                    AS "deprecated_count",
        100.0 * COUNT(*) FILTER (WHERE has_deprecated_siret) / COUNT(*) AS "deprecated_percentage"
    FROM structures
    GROUP BY 1
    ORDER BY 3 DESC
)

SELECT * FROM final
