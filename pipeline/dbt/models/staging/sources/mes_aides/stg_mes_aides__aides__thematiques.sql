WITH source AS (
    {{ stg_source_header('mes_aides', 'aides') }}),

final AS (
    SELECT
        data ->> 'ID'                                                                         AS "aide_id",
        LOWER(SUBSTRING(UNNEST(STRING_TO_ARRAY(data ->> 'Thématiques', ',')) FROM '. (.*)')) AS "value"
    FROM source
)

SELECT * FROM final
