WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}
),

final AS (
    SELECT
        data ->> 'lieu_id'                                                AS "lieu_id",
        TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'publics' -> 'familiale')) AS "value"
    FROM source
    WHERE
        data -> 'publics' -> 'familiale' IS NOT NULL
        AND data #>> '{position,country}' = 'fr' AND NOT data -> 'sources' @> '[{"name": "dora"}]'
)

SELECT * FROM final
