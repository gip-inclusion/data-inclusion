WITH source AS (
    {{ stg_source_header('mes_aides', 'aides') }}),

final AS (
    SELECT
        data ->> 'id'                                                       AS "aide_id",
        NULLIF(LOWER(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'thematiques')), '') AS "value"
    FROM source
)

SELECT * FROM final
