WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}
),

lieux AS (
    SELECT
        _di_source_id                                                 AS "_di_source_id",
        CAST(data ->> 'updatedAt' AS DATE)                            AS "updated_at",
        CAST(data #>> '{position,location,coordinates,0}' AS FLOAT)   AS "position__coordinates__x",
        CAST(data #>> '{position,location,coordinates,1}' AS FLOAT)   AS "position__coordinates__y",
        CAST(data #>> '{modalities,inconditionnel}' AS BOOLEAN)       AS "modalities__inconditionnel",
        CAST(data #>> '{modalities,appointment,checked}' AS BOOLEAN)  AS "modalities__appointment__checked",
        CAST(data #>> '{modalities,inscription,checked}' AS BOOLEAN)  AS "modalities__inscription__checked",
        CAST(data #>> '{modalities,orientation,checked}' AS BOOLEAN)  AS "modalities__orientation__checked",
        data ->> 'lieu_id'                                            AS "id",
        data ->> 'lieu_id'                                            AS "lieu_id",
        -- TODO: entity.phones
        data ->> 'name'                                                AS "name",
        data #>> '{position,city}'                                     AS "position__city",
        data #>> '{position,cityCode}'                                 AS "position__city_code",
        data #>> '{position,country}'                                  AS "position__country",
        data ->> 'description'                                         AS "description",
        data ->> 'seo_url'                                             AS "seo_url",
        data #>> '{position,postalCode}'                               AS "position__postal_code",
        data #>> '{position,address}'                                  AS "position__address",
        NULLIF(TRIM(data #>> '{position,additionalInformation}'), '')  AS "position__additional_information",
        data #>> '{position,department}'                               AS "position__department",
        data #>> '{publics,age}'                                       AS "publics__age",
        CAST(data #>> '{publics,accueil}' AS INT)                      AS "publics__accueil",
        NULLIF(data #>> '{entity,mail}', '')                           AS "entity_mail",
        NULLIF(data #>> '{entity,website}', '')                        AS "entity_website",
        data -> 'newhours'                                             AS "newhours",
        data #>> '{modalities,appointment,precisions}'                 AS "modalities__appointment__precisions",
        data #>> '{modalities,inscription,precisions}'                 AS "modalities__inscription__precisions",
        data #>> '{modalities,orientation,precisions}'                 AS "modalities__orientation__precisions",
        data -> 'sources'                                              AS "sources"
    FROM source
),

final AS (
    SELECT *
    FROM lieux
    WHERE position__country = 'fr' AND NOT sources @> '[{"name": "dora"}]'
)

SELECT * FROM final
