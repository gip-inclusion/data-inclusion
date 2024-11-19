WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}
),

final AS (
    SELECT
        data ->> 'lieu_id'                                                     AS "lieu_id",
        TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'publics' -> 'administrative')) AS "value"
    FROM source
    WHERE data -> 'publics' -> 'administrative' IS NOT NULL
)

SELECT * FROM final
