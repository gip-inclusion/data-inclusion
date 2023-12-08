WITH source AS (
    {{ stg_source_header('agefiph', 'services') }}
),

final AS (
    SELECT
        source._di_source_id  AS "_di_source_id",
        source.data ->> 'id'  AS "service_id",
        publics.data ->> 'id' AS "public_id"
    FROM
        source,
        LATERAL(SELECT * FROM JSONB_PATH_QUERY(source.data, '$.relationships.field_publics_cibles.data[*]')) AS publics (data)
    WHERE
        publics.data ->> 'id' IS NOT NULL
)

SELECT * FROM final
