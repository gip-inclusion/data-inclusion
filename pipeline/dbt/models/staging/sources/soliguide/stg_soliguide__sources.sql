WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}),

lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

sources AS (
    SELECT
        source.data ->> 'lieu_id' AS "lieu_id",
        sources.data ->> 'name'   AS "name"
    FROM
        source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.sources[*]') AS sources (data)
),

final AS (
    SELECT sources.*
    FROM sources
    INNER JOIN lieux ON sources.lieu_id = lieux.lieu_id
)

SELECT * FROM final
