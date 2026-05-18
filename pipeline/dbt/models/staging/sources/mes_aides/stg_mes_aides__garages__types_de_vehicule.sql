WITH source AS (
    {{ stg_source_header('mes_aides', 'garages') }}),

final AS (
    SELECT
        data ->> 'id'                                                   AS "garage_id",
        LOWER(TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'typesVehicule'))) AS "item"
    FROM source
)

SELECT * FROM final
