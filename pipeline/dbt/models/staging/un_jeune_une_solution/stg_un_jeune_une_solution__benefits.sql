WITH source AS (
    SELECT * FROM {{ source('un_jeune_une_solution', 'benefits') }}
),

final AS (
    SELECT
        _di_source_id               AS "_di_source_id",
        data ->> 'id'               AS "id",
        data ->> 'slug'             AS "slug",
        data ->> 'label'            AS "label",
        data ->> 'description'      AS "description",
        data ->> 'type'             AS "type",
        data ->> 'source'           AS "source",
        data #>> '{institution,id}' AS "institution_id"
    FROM source
)

SELECT * FROM final
