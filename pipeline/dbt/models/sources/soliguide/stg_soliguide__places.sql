WITH source AS (
    SELECT *
    FROM {{ source('soliguide', 'places') }}
),

final AS (
    SELECT
        "lieu_id"::TEXT                                    AS "id",
        "lieu_id"::TEXT                                    AS "lieu_id",
        "name"                                             AS "name",
        "ville"                                            AS "ville",
        "description"                                      AS "description",
        "updatedAt"::DATE                                  AS "updated_at",
        "seo_url"                                          AS "seo_url",
        ("position" #>> '{position,coordinates,x}')::FLOAT AS "position_coordinates_x",
        ("position" #>> '{position,coordinates,y}')::FLOAT AS "position_coordinates_y",
        -- TODO: entity.phones
        "position" ->> 'codePostal'                        AS "position_code_postal",
        "position" ->> 'adresse'                           AS "position_adresse",
        "position" ->> 'complementAdresse'                 AS "position_complement_adresse",
        NULLIF("entity" ->> 'mail', '')                    AS "entity_mail",
        "entity" ->> 'website'                             AS "entity_website"
    FROM source
)

SELECT * FROM final
