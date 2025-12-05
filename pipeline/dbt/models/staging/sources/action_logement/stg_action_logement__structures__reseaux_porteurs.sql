WITH source AS (
    {{ stg_source_header('action_logement', 'structures') }}),

final AS (
    SELECT
        source.data ->> 'id'                                              AS "structure_id",
        UNNEST(STRING_TO_ARRAY(source.data ->> 'reseaux_porteurs', ', ')) AS "value"
    FROM source
    WHERE
        NOT COALESCE(CAST(source.data ->> '__ignore__' AS BOOLEAN), FALSE)
)

SELECT * FROM final
