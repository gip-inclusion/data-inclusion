WITH source AS (
    {{ stg_source_header('mes_aides', 'aides') }}),

final AS (
    SELECT
        data ->> 'ID' AS "aide_id",
        UNNEST(REGEXP_SPLIT_TO_ARRAY(
            -- Ignore commas inside double quotes. Example: "foo, bar", baz
            data ->> 'Justificatifs',
            ',(?=(?:[^"]*"[^"]*")*[^"]*$)'
        ))            AS "value"
    FROM source

)

SELECT * FROM final
