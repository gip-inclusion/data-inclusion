WITH source AS (
    SELECT *
    FROM {{ source('soliguide', 'places') }}
),

final AS (
    SELECT
        -- services do not have an id
        NULL                          AS "id",
        source."lieu_id"::TEXT        AS "lieu_id",
        services.data ->> 'name'      AS "name",
        services.data ->> 'categorie' AS "categorie"
    FROM
        source,
        LATERAL(SELECT * FROM JSONB_PATH_QUERY(source.services_all, '$[*]')) AS services (data)
)

SELECT * FROM final
