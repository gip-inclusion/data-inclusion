WITH final AS (
    SELECT DISTINCT site_web AS "url"
    FROM {{ ref('int__union_structures') }}
    WHERE site_web IS NOT NULL
    UNION
    SELECT DISTINCT prise_rdv AS "url"
    FROM {{ ref('int__union_services') }}
    WHERE prise_rdv IS NOT NULL
    UNION
    SELECT DISTINCT formulaire_en_ligne AS "url"
    FROM {{ ref('int__union_services') }}
    WHERE formulaire_en_ligne IS NOT NULL
    UNION
    SELECT DISTINCT page_web AS "url"
    FROM {{ ref('int__union_services') }}
    WHERE page_web IS NOT NULL
)

SELECT * FROM final
ORDER BY url
