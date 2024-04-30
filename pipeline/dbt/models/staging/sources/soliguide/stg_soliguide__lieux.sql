WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}
),

lieux AS (
    SELECT
        _di_source_id                                                 AS "_di_source_id",
        (data ->> 'updatedAt')::DATE                                  AS "updated_at",
        (data #>> '{position,location,coordinates,0}')::FLOAT         AS "position__coordinates__x",
        (data #>> '{position,location,coordinates,1}')::FLOAT         AS "position__coordinates__y",
        (data #>> '{modalities,inconditionnel}')::BOOLEAN             AS "modalities__inconditionnel",
        (data #>> '{modalities,appointment,checked}')::BOOLEAN        AS "modalities__appointment__checked",
        (data #>> '{modalities,inscription,checked}')::BOOLEAN        AS "modalities__inscription__checked",
        (data #>> '{modalities,orientation,checked}')::BOOLEAN        AS "modalities__orientation__checked",
        data ->> 'lieu_id'                                            AS "id",
        data ->> 'lieu_id'                                            AS "lieu_id",
        -- TODO: entity.phones
        data ->> 'name'                                               AS "name",
        data #>> '{position,city}'                                    AS "position__city",
        data #>> '{position,cityCode}'                                AS "position__city_code",
        data #>> '{position,country}'                                 AS "position__country",
        data ->> 'description'                                        AS "description",
        data ->> 'seo_url'                                            AS "seo_url",
        data #>> '{position,postalCode}'                              AS "position__postal_code",
        data #>> '{position,address}'                                 AS "position__address",
        NULLIF(TRIM(data #>> '{position,additionalInformation}'), '') AS "position__additional_information",
        data #>> '{position,department}'                              AS "position__department",
        NULLIF(data #>> '{entity,mail}', '')                          AS "entity_mail",
        NULLIF(data #>> '{entity,website}', '')                       AS "entity_website",
        data -> 'newhours'                                            AS "newhours",
        data #>> '{modalities,appointment,precisions}'                AS "modalities__appointment__precisions",
        data #>> '{modalities,inscription,precisions}'                AS "modalities__inscription__precisions",
        data #>> '{modalities,orientation,precisions}'                AS "modalities__orientation__precisions"
    FROM source
),

final AS (
    SELECT *
    FROM lieux
    WHERE position__country = 'fr'
)

SELECT * FROM final
