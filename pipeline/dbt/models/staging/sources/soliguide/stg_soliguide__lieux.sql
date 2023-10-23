{% set source_model = source('soliguide', 'lieux') %}

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
        _di_source_id                                         AS "_di_source_id",
        (data ->> 'updatedAt')::DATE                          AS "updated_at",
        (data #>> '{position,location,coordinates,0}')::FLOAT AS "position_coordinates_x",
        (data #>> '{position,location,coordinates,1}')::FLOAT AS "position_coordinates_y",
        data ->> 'lieu_id'                                    AS "id",
        data ->> 'lieu_id'                                    AS "lieu_id",
        data ->> 'name'                                       AS "name",
        data #>> '{position,ville}'                           AS "position_ville",
        data ->> 'description'                                AS "description",
        data ->> 'seo_url'                                    AS "seo_url",
        -- TODO: entity.phones
        data #>> '{position,codePostal}'                      AS "position_code_postal",
        data #>> '{position,adresse}'                         AS "position_adresse",
        data #>> '{position,complementAdresse}'               AS "position_complement_adresse",
        data #>> '{position,departement}'                     AS "departement",
        NULLIF(data #>> '{entity,mail}', '')                  AS "entity_mail",
        data #>> '{entity,website}'                           AS "entity_website",
        data -> 'newhours'                                    AS "newhours"
    FROM source
)

SELECT * FROM final
