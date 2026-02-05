WITH source AS (
    {{ stg_source_header('dora', 'structures') }}),

final AS (
    SELECT
        NULLIF(TRIM(data ->> 'id'), '')                                      AS "structure_id",
        NULLIF(TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'labels_autres')), '') AS "item"
    FROM source
)

SELECT final.*
FROM final
WHERE final.item IS NOT NULL
