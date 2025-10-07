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
        data ->> 'description'                                                                           AS "description",
        data ->> 'seo_url'                                                                               AS "seo_url",
        data #>> '{position,postalCode}'                                                                 AS "position__postal_code",
        data #>> '{position,address}'                                                                    AS "position__address",
        NULLIF(TRIM(data #>> '{position,additionalInformation}'), '')                                    AS "position__additional_information",
        data #>> '{position,department}'                                                                 AS "position__department",
        data #>> '{publics,age}'                                                                         AS "publics__age",
        CAST(data #>> '{publics,accueil}' AS INT)                                                        AS "publics__accueil",
        NULLIF(data #>> '{entity,mail}', '')                                                             AS "entity_mail",
        NULLIF(data #>> '{entity,website}', '')                                                          AS "entity_website",
        data #>> '{tempInfos,message,name}'                                                              AS "temp_infos__message__texte",
        data -> 'newhours'                                                                               AS "newhours",
        data #>> '{modalities,appointment,precisions}'                                                   AS "modalities__appointment__precisions",
        data #>> '{modalities,inscription,precisions}'                                                   AS "modalities__inscription__precisions",
        data #>> '{modalities,orientation,precisions}'                                                   AS "modalities__orientation__precisions",
        data -> 'sources'                                                                                AS "sources"
    FROM source
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
deduplicated_lieux AS (
    SELECT DISTINCT lieu_id
    FROM (
        SELECT DISTINCT ON (1)
            services.data ->> 'serviceObjectId' AS service_id,
            lieux.updated_at,
            lieux.lieu_id
        FROM
            lieux
        LEFT JOIN source ON lieux.lieu_id = source.data ->> 'lieu_id',
            LATERAL JSONB_PATH_QUERY(source.data, '$.services_all[*]') AS services (data)
        ORDER BY 1, 2 DESC
    )
),

final AS (
    SELECT lieux.*
    FROM lieux
    INNER JOIN deduplicated_lieux ON lieux.lieu_id = deduplicated_lieux.lieu_id
    WHERE
        lieux.position__country = 'fr'
        AND NOT lieux.sources @> '[{"name": "dora"}]'
)

SELECT * FROM final
