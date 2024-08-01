WITH source AS (
    {{ stg_source_header('fredo', 'structures') }}
),

final AS (
    SELECT
        data ->> 'id'                                  AS "structure_id",
        JSONB_ARRAY_ELEMENTS_TEXT(data -> 'quartiers') AS "value"
    FROM source
    WHERE data ->> 'quartiers' IS NOT NULL
)

SELECT * FROM final
