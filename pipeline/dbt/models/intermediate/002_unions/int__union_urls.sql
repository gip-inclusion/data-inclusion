WITH site_web AS (
    SELECT DISTINCT site_web AS "url"
    FROM {{ ref('int__union_structures') }}
    WHERE site_web IS NOT NULL
),

accessibilite AS (
    SELECT DISTINCT accessibilite AS "url"
    FROM {{ ref('int__union_structures') }}
    WHERE accessibilite IS NOT NULL
),

final AS (
    SELECT * FROM site_web
    UNION
    SELECT * FROM accessibilite
    ORDER BY url
)

SELECT * FROM final
