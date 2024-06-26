{% set source_model = source('insee', 'etat_civil_prenoms') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    ),

{% else %}

    WITH source AS (
        SELECT
            NULL                 AS "preusuel",
            NULL                 AS "annais",
            CAST(NULL AS BIGINT) AS "sexe",
            CAST(NULL AS BIGINT) AS "nombre"
        WHERE FALSE
    ),

{% endif %}

final AS (
    SELECT DISTINCT LOWER(preusuel) AS prenom
    FROM source
    WHERE
        LENGTH(preusuel) > 2
        AND nombre > 100
        AND preusuel != 'SAINT'
)

SELECT * FROM final
