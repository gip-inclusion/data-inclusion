WITH source AS (
    {{ stg_source_header('mes_aides', 'garages') }}),

final AS (
    SELECT
        data ->> 'ID'                                          AS "garage_id",
        LOWER(TRIM(STRING_TO_TABLE(data ->> 'Services', ','))) AS "item"
    FROM source
)

SELECT * FROM final
