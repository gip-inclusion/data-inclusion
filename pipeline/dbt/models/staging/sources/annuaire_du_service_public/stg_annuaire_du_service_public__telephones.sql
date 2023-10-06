{% set source_model = source('annuaire_du_service_public', 'etablissements') %}

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
        source.data ->> 'id'              AS "etablissement_id",
        telephones.data ->> 'valeur'      AS "valeur",
        telephones.data ->> 'description' AS "description"
    FROM
        source,
        LATERAL(SELECT * FROM JSONB_PATH_QUERY(source.data, '$.telephone[*]')) AS telephones (data)
)

SELECT * FROM final
