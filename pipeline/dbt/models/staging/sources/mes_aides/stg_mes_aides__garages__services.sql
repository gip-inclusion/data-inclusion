WITH source AS (
    {{ stg_source_header('mes_aides', 'garages') }}),

final AS (
    SELECT
        data ->> 'id'                                                        AS "garage_id",
        UNACCENT(LOWER(TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'services')))) AS "item"
    FROM source
)

SELECT * FROM final
