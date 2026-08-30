WITH source AS (
    {{ stg_source_header('bpifrance', 'structures') }}),

final AS (
    SELECT
        NULLIF(TRIM(data ->> 'id'), '')                                         AS "structure_id",
        NULLIF(TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'reseaux_porteurs')), '') AS "item"
    FROM source
    WHERE data ->> 'reseaux_porteurs' IS NOT NULL
)

SELECT final.*
FROM final
