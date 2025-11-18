WITH source AS (
    {{ stg_source_header('mes_aides', 'garages') }}),

final AS (
    SELECT
        data -> 'fields' ->> 'ID'                                                  AS "garage_id",
        LOWER(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'fields' -> 'Types de véhicule')) AS "item"
    FROM source
)

SELECT * FROM final
