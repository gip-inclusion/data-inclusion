WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}),

lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

-- about timestamps : soliguide can have corrupted timestamps,
-- therefore timestamps are extracted from datetime fields and then casted to date

services AS (
    SELECT
        source.data ->> 'lieu_id'                                                                                         AS "lieu_id",
        services.data ->> 'serviceObjectId'                                                                               AS "id",
        services.data ->> 'category'                                                                                      AS "category",
        processings.html_to_markdown(NULLIF(TRIM(services.data ->> 'description'), ''))                                   AS "description",
        services.data -> 'hours'                                                                                          AS "hours",
        LOWER(services.data #>> '{saturated,status}')                                                                     AS "saturated__status",
        CAST(services.data ->> 'differentHours' AS BOOLEAN)                                                               AS "different_hours",
        CAST(services.data #>> '{close,actif}' AS BOOLEAN)                                                                AS "close__actif",
        CAST(SUBSTRING(services.data #>> '{close,dateDebut}' FROM '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z') AS DATE) AS "close__date_debut",
        CAST(SUBSTRING(services.data #>> '{close,dateFin}' FROM '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z') AS DATE)   AS "close__date_fin",
        processings.html_to_markdown(NULLIF(TRIM(services.data #>> '{modalities,other}'), ''))                            AS "modalities__other",
        CAST(services.data #>> '{modalities,inconditionnel}' AS BOOLEAN)                                                  AS "modalities__inconditionnel",
        CAST(services.data #>> '{modalities,appointment,checked}' AS BOOLEAN)                                             AS "modalities__appointment__checked",
        NULLIF(TRIM(services.data #>> '{modalities,appointment,precisions}'), '')                                         AS "modalities__appointment__precisions",
        CAST(services.data #>> '{modalities,price,checked}' AS BOOLEAN)                                                   AS "modalities__price__checked",
        NULLIF(TRIM(services.data #>> '{modalities,price,precisions}'), '')                                               AS "modalities__price__precisions",
        CAST(services.data #>> '{modalities,inscription,checked}' AS BOOLEAN)                                             AS "modalities__inscription__checked",
        NULLIF(TRIM(services.data #>> '{modalities,inscription,precisions}'), '')                                         AS "modalities__inscription__precisions",
        CAST(services.data #>> '{modalities,orientation,checked}' AS BOOLEAN)                                             AS "modalities__orientation__checked",
        NULLIF(TRIM(services.data #>> '{modalities,orientation,precisions}'), '')                                         AS "modalities__orientation__precisions",
        NULLIF(TRIM(services.data #>> '{publics,description}'), '')                                                       AS "publics__description",
        CAST(services.data #>> '{publics,accueil}' AS INT)                                                                AS "publics__accueil",
        CAST(services.data #>> '{differentModalities}' AS BOOLEAN)                                                        AS "different_modalities",
        CAST(services.data #>> '{differentPublics}' AS BOOLEAN)                                                           AS "different_publics"
    FROM
        source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.services_all[*]') AS services (data)
),

final AS (
    SELECT services.*
    FROM services
    INNER JOIN lieux ON services.lieu_id = lieux.lieu_id
)

SELECT * FROM final
