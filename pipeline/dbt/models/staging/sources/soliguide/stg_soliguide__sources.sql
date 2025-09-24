WITH lieux AS (
    {{ stg_source_header('soliguide', 'lieux') }}),

sources AS (
    SELECT
        lieux.data ->> 'lieu_id' AS "lieu_id",
        sources.data ->> 'name'  AS "source_name"
    FROM
        lieux,
        LATERAL JSONB_PATH_QUERY(lieux.data, '$.sources[*]') AS sources (data)
    WHERE lieux.data #>> '{position,country}' = 'fr' AND NOT lieux.data -> 'sources' @> '[{"name": "dora"}]'
),

final AS (
    SELECT *
    FROM sources
)

SELECT * FROM final
