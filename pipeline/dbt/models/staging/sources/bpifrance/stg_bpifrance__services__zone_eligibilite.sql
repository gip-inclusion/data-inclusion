WITH source AS (
    {{ stg_source_header('bpifrance', 'services') }}),

final AS (
    SELECT
        NULLIF(TRIM(data ->> 'id'), '')                                         AS "service_id",
        NULLIF(TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'zone_eligibilite')), '') AS "item"
    FROM source
    WHERE data ->> 'zone_eligibilite' IS NOT NULL
)

SELECT final.*
FROM final
