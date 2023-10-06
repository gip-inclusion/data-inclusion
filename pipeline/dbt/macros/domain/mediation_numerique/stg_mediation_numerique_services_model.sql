{% macro stg_mediation_numerique_services_model() %}

{% set source_model = source('mediation_numerique_' ~ model.fqn[-2], 'services') %}

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
        _di_source_id                                                                                          AS "_di_source_id",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'types', 'null'))) AS TEXT [])       AS "types",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'profils', 'null'))) AS TEXT [])     AS "profils",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'thematiques', 'null'))) AS TEXT []) AS "thematiques",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'frais', 'null'))) AS TEXT [])       AS "frais",
        CAST((data ->> 'longitude') AS FLOAT)                                                                  AS "longitude",
        CAST((data ->> 'latitude') AS FLOAT)                                                                   AS "latitude",
        data ->> 'id'                                                                                          AS "id",
        data ->> 'structure_id'                                                                                AS "structure_id",
        data ->> 'nom'                                                                                         AS "nom",
        data ->> 'source'                                                                                      AS "source",
        data ->> 'prise_rdv'                                                                                   AS "prise_rdv"
    FROM source
)

SELECT * FROM final

{% endmacro %}