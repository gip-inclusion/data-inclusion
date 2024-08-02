{% set source_model = source('internal', 'extra__geocoded_results') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    ),

{% else %}

WITH source AS (
    SELECT
        NULL                AS "_di_surrogate_id",
        NULL                AS "adresse",
        NULL                AS "code_postal",
        NULL                AS "commune",
        CAST(NULL AS FLOAT) AS "latitude",
        CAST(NULL AS FLOAT) AS "longitude",
        NULL                AS "result_label",
        CAST(NULL AS FLOAT) AS "result_score",
        NULL                AS "result_score_next",
        NULL                AS "result_type",
        NULL                AS "result_id",
        NULL                AS "result_housenumber",
        NULL                AS "result_name",
        NULL                AS "result_street",
        NULL                AS "result_postcode",
        NULL                AS "result_city",
        NULL                AS "result_context",
        NULL                AS "result_citycode",
        NULL                AS "result_oldcitycode",
        NULL                AS "result_oldcity",
        NULL                AS "result_district",
        NULL                AS "result_status"
    WHERE FALSE
),

{% endif %}

final AS (
    SELECT *
    FROM source
    WHERE result_id IS NOT NULL
)

SELECT * FROM final
