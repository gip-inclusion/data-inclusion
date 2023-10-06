{% set source_model = source('mes_aides', 'aides') %}

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
        _di_source_id                                              AS "_di_source_id",
        (data #>> '{fields,Ville Longitude}')::FLOAT               AS "ville_longitude",
        (data #>> '{fields,Ville Latitude}')::FLOAT                AS "ville_latitude",
        (data #>> '{fields,Modifié le}')::TIMESTAMP WITH TIME ZONE AS "modifie_le",
        data #>> '{fields,ID}'                                     AS "id",
        data #>> '{fields,Nom}'                                    AS "nom",
        data #>> '{fields,Ville Nom}'                              AS "ville_nom",
        data #>> '{fields,Code Postal}'                            AS "code_postal",
        data #>> '{fields,Adresse}'                                AS "adresse",
        data #>> '{fields,Téléphone}'                              AS "telephone",
        data #>> '{fields,Email}'                                  AS "email",
        data #>> '{fields,Url}'                                    AS "url"
    FROM source
)

SELECT * FROM final
