WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}),

lieux AS (
    SELECT
        data ->> 'lieu_id'                                                                               AS "lieu_id",
        CAST(SUBSTRING(data ->> 'updatedAt' FROM '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z') AS DATE) AS "updated_at",
        CAST(data #>> '{position,location,coordinates,0}' AS FLOAT)                                      AS "position__coordinates__x",
        CAST(data #>> '{position,location,coordinates,1}' AS FLOAT)                                      AS "position__coordinates__y",
        CAST(data #>> '{modalities,inconditionnel}' AS BOOLEAN)                                          AS "modalities__inconditionnel",
        CAST(data #>> '{modalities,appointment,checked}' AS BOOLEAN)                                     AS "modalities__appointment__checked",
        CAST(data #>> '{modalities,inscription,checked}' AS BOOLEAN)                                     AS "modalities__inscription__checked",
        CAST(data #>> '{modalities,orientation,checked}' AS BOOLEAN)                                     AS "modalities__orientation__checked",
        data ->> 'lieu_id'                                                                               AS "id",
        -- first replace trailing groups of 2 or more dots by an ellipsis
        -- then remove trailing dot if not preceded by "etc"
        REGEXP_REPLACE(REGEXP_REPLACE(data ->> 'name', '\.{2,}$', '…'), '(?<!etc)\.$', '')             AS "name",
        data #>> '{position,city}'                                                                       AS "position__city",
        data #>> '{position,cityCode}'                                                                   AS "position__city_code",
        data #>> '{position,country}'                                                                    AS "position__country",
        NULLIF(TRIM(processings.html_to_markdown(data ->> 'description')), '')                           AS "description",
        NULLIF(TRIM(data ->> 'seo_url'), '')                                                             AS "seo_url",
        data #>> '{position,postalCode}'                                                                 AS "position__postal_code",
        NULLIF(BTRIM(REGEXP_REPLACE(data #>> '{position,address}', ', \d\d\d\d\d.*$', ''), ','), '')     AS "position__address",
        NULLIF(TRIM(data #>> '{position,additionalInformation}'), '')                                    AS "position__additional_information",
        data #>> '{position,department}'                                                                 AS "position__department",
        data #>> '{position,departmentCode}'                                                             AS "position__department_code",
        CAST(data #>> '{publics,accueil}' AS INT)                                                        AS "publics__accueil",
        CAST(data #>> '{publics,age,min}' AS INT)                                                        AS "publics__age__min",
        CAST(data #>> '{publics,age,max}' AS INT)                                                        AS "publics__age__max",
        NULLIF(TRIM(data #>> '{publics,description}'), '')                                               AS "publics__description",
        NULLIF(TRIM(data #>> '{entity,mail}'), '')                                                       AS "entity_mail",
        NULLIF(TRIM(data #>> '{entity,website}'), '')                                                    AS "entity_website",
        NULLIF(TRIM(data #>> '{tempInfos,message,name}'), '')                                            AS "temp_infos__message__name",
        CAST(data #>> '{tempInfos,closure,actif}' AS BOOLEAN)                                            AS "temp_infos__closure__actif",
        CAST(data #>> '{tempInfos,hours,actif}' AS BOOLEAN)                                              AS "temp_infos__hours__actif",
        data #> '{tempInfos,hours,hours}'                                                                AS "temp_infos__hours__hours",
        data -> 'newhours'                                                                               AS "newhours",
        NULLIF(TRIM(processings.html_to_markdown(data #>> '{modalities,other}')), '')                    AS "modalities__other",
        NULLIF(TRIM(data #>> '{modalities,price,precisions}'), '')                                       AS "modalities__price__precisions",
        NULLIF(TRIM(data #>> '{modalities,appointment,precisions}'), '')                                 AS "modalities__appointment__precisions",
        NULLIF(TRIM(data #>> '{modalities,inscription,precisions}'), '')                                 AS "modalities__inscription__precisions",
        NULLIF(TRIM(data #>> '{modalities,orientation,precisions}'), '')                                 AS "modalities__orientation__precisions",
        CAST(data #>> '{modalities,pmr,checked}' AS BOOLEAN)                                             AS "modalities__pmr__checked"
    FROM source
    WHERE
        NOT data -> 'sources' @> '[{"name": "dora"}]'
),

/*
    Starting mid 2025, there are duplicated lieux in Soliguide. For instance,
    https://soliguide.fr/fr/fiche/62249 and https://soliguide.fr/fr/fiche/55997.

    These are two seperate entries in Soliguide with different `lieu_id` but
    representing the same physical location (same coordinates, same name, etc.).

    For now, duplicates can be identified by looking at the services they offer.
    Each service has a unique `serviceObjectId` that is consistent across
    duplicated lieux. Though this might not be enough if the two entries differ.

    Below, entries are deduplicated by keeping the most recently updated one.
    The remaining entries can be used to filter the `lieux` table.
*/
services AS (
    SELECT
        lieux.lieu_id,
        lieux.updated_at,
        services.data ->> 'serviceObjectId' AS service_id
    FROM
        lieux
    LEFT JOIN source ON lieux.lieu_id = source.data ->> 'lieu_id',
        LATERAL JSONB_PATH_QUERY(source.data, '$.services_all[*]') AS services (data)
),

duplicates AS (
    SELECT DISTINCT duplicates.lieu_id
    FROM
        services
    INNER JOIN services AS duplicates
        ON
            services.service_id = duplicates.service_id
            AND services.updated_at > duplicates.updated_at
),

final AS (
    SELECT lieux.*
    FROM lieux
    LEFT JOIN duplicates ON lieux.lieu_id = duplicates.lieu_id
    WHERE
        lieux.position__country = 'fr'
        AND duplicates.lieu_id IS NULL
)

SELECT * FROM final
