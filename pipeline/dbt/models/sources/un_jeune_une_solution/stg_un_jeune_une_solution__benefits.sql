WITH source AS (
    SELECT *
    FROM {{ source('data_inclusion', 'datalake') }}
    WHERE
        logical_date = '{{ var('logical_date') }}'
        AND src_alias = '1jeune1solution'
        AND file ~ 'benefits'
),

final AS (
    SELECT
        logical_date,
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
