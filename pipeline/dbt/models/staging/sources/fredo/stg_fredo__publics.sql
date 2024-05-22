WITH source AS (
    {{ stg_source_header('fredo', 'structures') }}
),

final AS (
    SELECT
        data ->> 'id'                                             AS "id",
        LOWER(TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'publics'))) AS "value"
    FROM source
    WHERE data ->> 'publics' IS NOT NULL
)

SELECT * FROM final
