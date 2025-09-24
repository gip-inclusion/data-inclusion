WITH final AS (
    SELECT DISTINCT accessibilite_lieu AS "url"
    FROM {{ ref('int__union_structures_v1') }}
    WHERE accessibilite_lieu IS NOT NULL
    UNION
    SELECT DISTINCT lien_mobilisation AS "url"
    FROM {{ ref('int__union_services_v1') }}
    WHERE lien_mobilisation IS NOT NULL
)

SELECT * FROM final
ORDER BY url
