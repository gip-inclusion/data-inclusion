WITH source AS (
    {{ stg_source_header('mes_aides', 'aides') }}),

final AS (
    SELECT
        data ->> 'ID'                                                                                                         AS "aide_id",
        UNACCENT(TRIM(LOWER(REGEXP_REPLACE(UNNEST(STRING_TO_ARRAY(data ->> 'Type d''aide', ',')), '\(ex : .+\)$', '', 'g')))) AS "value"
    FROM source
)

SELECT * FROM final
