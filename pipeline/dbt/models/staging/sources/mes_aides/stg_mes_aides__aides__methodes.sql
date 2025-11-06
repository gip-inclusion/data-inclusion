WITH source AS (
    {{ stg_source_header('mes_aides', 'aides') }}),

final AS (
    SELECT
        data ->> 'ID'                                     AS "aide_id",
        UNNEST(STRING_TO_ARRAY(data ->> 'Méthode', ',')) AS "value"
    FROM source
)

SELECT * FROM final
