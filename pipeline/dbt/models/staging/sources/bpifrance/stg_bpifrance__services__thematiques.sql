WITH source AS (
    {{ stg_source_header('bpifrance', 'services') }}),

final AS (
    SELECT
        NULLIF(TRIM(data ->> 'id'), '')                                    AS "service_id",
        NULLIF(TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'thematiques')), '') AS "item"
    FROM source
    WHERE data ->> 'thematiques' IS NOT NULL
)

SELECT final.*
FROM final
