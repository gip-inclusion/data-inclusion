WITH source AS (
    {{ stg_source_header('bpifrance', 'services') }}),

final AS (
    SELECT
        NULLIF(TRIM(data ->> 'id'), '')                                        AS "service_id",
        NULLIF(TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'mobilisable_par')), '') AS "item"
    FROM source
    WHERE data ->> 'mobilisable_par' IS NOT NULL
)

SELECT final.*
FROM final
