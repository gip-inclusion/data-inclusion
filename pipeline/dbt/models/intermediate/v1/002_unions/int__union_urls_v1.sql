WITH final AS (
    SELECT DISTINCT accessibilite_lieu AS "url"
    FROM {{ ref('int__union_structures_v1') }}
    WHERE accessibilite_lieu IS NOT NULL
    UNION
    SELECT DISTINCT lien_source AS "url"
    FROM {{ ref('int__union_structures_v1') }}
    WHERE lien_source IS NOT NULL
    UNION
    SELECT DISTINCT site_web AS "url"
    FROM {{ ref('int__union_structures_v1') }}
    WHERE site_web IS NOT NULL
    UNION
    SELECT DISTINCT lien_mobilisation AS "url"
    FROM {{ ref('int__union_services_v1') }}
    WHERE lien_mobilisation IS NOT NULL
    UNION
    SELECT DISTINCT lien_source AS "url"
    FROM {{ ref('int__union_services_v1') }}
    WHERE lien_source IS NOT NULL
)

SELECT * FROM final
ORDER BY url
