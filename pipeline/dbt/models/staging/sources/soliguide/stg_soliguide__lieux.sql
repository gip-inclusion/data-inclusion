WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}
),

final AS (
    SELECT
        _di_source_id                                          AS "_di_source_id",
        (data ->> 'updatedAt')::DATE                           AS "updated_at",
        (data #>> '{position,location,coordinates,0}')::FLOAT  AS "position_coordinates_x",
        (data #>> '{position,location,coordinates,1}')::FLOAT  AS "position_coordinates_y",
        (data #>> '{modalities,inconditionnel}')::BOOLEAN      AS "modalities__inconditionnel",
        (data #>> '{modalities,appointment,checked}')::BOOLEAN AS "modalities__appointment__checked",
        (data #>> '{modalities,inscription,checked}')::BOOLEAN AS "modalities__inscription__checked",
        (data #>> '{modalities,orientation,checked}')::BOOLEAN AS "modalities__orientation__checked",
        data ->> 'lieu_id'                                     AS "id",
        data ->> 'lieu_id'                                     AS "lieu_id",
        -- TODO: entity.phones
        data ->> 'name'                                        AS "name",
        data #>> '{position,ville}'                            AS "position_ville",
        data ->> 'description'                                 AS "description",
        data ->> 'seo_url'                                     AS "seo_url",
        data #>> '{position,codePostal}'                       AS "position_code_postal",
        data #>> '{position,adresse}'                          AS "position_adresse",
        data #>> '{position,complementAdresse}'                AS "position_complement_adresse",
        data #>> '{position,departement}'                      AS "departement",
        NULLIF(data #>> '{entity,mail}', '')                   AS "entity_mail",
        NULLIF(data #>> '{entity,website}', '')                AS "entity_website",
        data -> 'newhours'                                     AS "newhours",
        data #>> '{modalities,appointment,precisions}'         AS "modalities__appointment__precisions",
        data #>> '{modalities,inscription,precisions}'         AS "modalities__inscription__precisions",
        data #>> '{modalities,orientation,precisions}'         AS "modalities__orientation__precisions"
    FROM source
)

SELECT * FROM final
