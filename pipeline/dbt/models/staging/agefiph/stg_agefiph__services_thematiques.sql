WITH source AS (
    SELECT * FROM {{ source('agefiph', 'services') }}
),

final AS (
    SELECT
        source._di_source_id      AS "_di_source_id",
        source.data ->> 'id'      AS "service_id",
        thematiques.data ->> 'id' AS "thematique_id"
    FROM
        source,
        LATERAL(SELECT * FROM JSONB_PATH_QUERY(source.data, '$.relationships.field_thematique.data[*]')) AS thematiques (data)
    WHERE
        thematiques.data ->> 'id' IS NOT NULL
)

SELECT * FROM final