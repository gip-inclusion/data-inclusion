{% set source_model = source('siao', 'etablissements') %}

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
        _di_source_id                                                                             AS "_di_source_id",
        -- there is no proper index in the data, this is very problematic.
        -- for analytical use, annotate with the row number if the default ordering.
        CAST(ROW_NUMBER() OVER () AS TEXT)                                                        AS "id",
        NULLIF(NULLIF(REGEXP_REPLACE(data ->> 'Code SIRET', '\D', '', 'g'), REPEAT('0', 14)), '') AS "code_siret",
        data ->> 'Nom de la structure'                                                            AS "nom_de_la_structure",
        data ->> 'Ville'                                                                          AS "ville",
        NULLIF(LPAD(REGEXP_REPLACE(data ->> 'Code postal', '\D', '', 'g'), 5, '0'), '')           AS "code_postal",
        data ->> 'Adresse'                                                                        AS "adresse",
        data ->> 'Téléphone'                                                                      AS "telephone",
        data ->> 'Mail'                                                                           AS "mail"
    FROM source
)

SELECT * FROM final
