WITH source AS (
    {{ stg_source_header('dora', 'services') }}),

final AS (
    SELECT
        NULLIF(TRIM(data ->> 'id'), '')                                    AS "service_id",
        NULLIF(TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'thematiques')), '') AS "item"
    FROM source
)

SELECT * FROM final
