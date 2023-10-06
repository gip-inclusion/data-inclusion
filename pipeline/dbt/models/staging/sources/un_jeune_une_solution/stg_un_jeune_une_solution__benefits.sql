{% set source_model = source('un_jeune_une_solution', 'benefits') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    ),

{% else %}

WITH source AS (
    SELECT
        NULL                AS "_di_source_id",
        CAST(NULL AS JSONB) AS "data"
    WHERE FALSE
),

{% endif %}

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
