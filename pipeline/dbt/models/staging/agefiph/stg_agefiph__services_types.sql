WITH source AS (
    SELECT * FROM {{ source('agefiph', 'services') }}
),

final AS (
    SELECT
        source._di_source_id AS "_di_source_id",
        source.data ->> 'id' AS "service_id",
        types_.data ->> 'id' AS "type_id"
    FROM
        source,
        LATERAL(SELECT * FROM JSONB_PATH_QUERY(source.data, '$.relationships.field_type_aide_service.data[*]')) AS types_ (data)
    WHERE
        types_.data ->> 'id' IS NOT NULL
)

SELECT * FROM final
