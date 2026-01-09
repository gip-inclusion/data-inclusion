WITH source AS (
    {{ stg_source_header('fredo', 'structures') }}
),

final AS (
    SELECT
        data ->> 'id'                                           AS "structure_id",
        LOWER(TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'email'))) AS "value"
    FROM source
    WHERE data ->> 'email' IS NOT NULL
)

SELECT * FROM final
