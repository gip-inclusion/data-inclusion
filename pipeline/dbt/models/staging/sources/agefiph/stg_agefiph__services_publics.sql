{% set source_model = source('agefiph', 'services') %}

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
