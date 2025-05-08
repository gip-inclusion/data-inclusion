WITH final AS (
    SELECT DISTINCT site_web AS "url"
    FROM {{ ref('int__union_structures') }}
    WHERE site_web IS NOT NULL
    ORDER BY site_web
)

SELECT * FROM final
