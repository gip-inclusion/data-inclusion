WITH source AS (
    {{ stg_source_header('mes_aides', 'aides') }}),

final AS (
    SELECT
        data ->> 'ID'                                                        AS "aide_id",
        UNACCENT(LOWER(UNNEST(STRING_TO_ARRAY(data ->> 'Conditions', ',')))) AS "value"
    FROM source
)

SELECT * FROM final
