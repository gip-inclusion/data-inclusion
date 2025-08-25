WITH source AS (
    {{ stg_source_header('agefiph', 'services') }}
),

final AS (
    SELECT
        source.data ->> 'id'      AS "service_id",
        thematiques.data ->> 'id' AS "thematique_id"
    FROM
        source,
        LATERAL (SELECT th.* FROM JSONB_PATH_QUERY(source.data, '$.relationships.field_thematique.data[*]') AS th) AS thematiques (data)
    WHERE
        thematiques.data ->> 'id' IS NOT NULL
)

SELECT * FROM final
