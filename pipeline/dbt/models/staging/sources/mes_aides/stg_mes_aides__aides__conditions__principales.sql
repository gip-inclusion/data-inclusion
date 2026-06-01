WITH source AS (
    {{ stg_source_header('mes_aides', 'aides') }}),

final AS (
    SELECT
        data ->> 'id'                                                                     AS "aide_id",
        UNACCENT(LOWER(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'conditions' -> 'principales'))) AS "value"
    FROM source
)

SELECT * FROM final
