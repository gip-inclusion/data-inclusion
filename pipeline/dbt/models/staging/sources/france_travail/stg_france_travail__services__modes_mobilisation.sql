WITH source AS (
    {{ stg_source_header('france_travail', 'services') }}),

final AS (
    SELECT
        source.data ->> 'id'                                                AS "service_id",
        UNNEST(STRING_TO_ARRAY(source.data ->> 'modes_mobilisation', ', ')) AS "value"
    FROM source
    WHERE
        NOT COALESCE(CAST(source.data ->> '__ignore__' AS BOOLEAN), FALSE)
)

SELECT * FROM final
