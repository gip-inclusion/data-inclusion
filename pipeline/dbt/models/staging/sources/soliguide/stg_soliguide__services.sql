WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}
),

-- about timestamps : soliguide can have corrupted timestamps,
-- therefore timestamps are extracted from datetime fields and then casted to date

services AS (
    SELECT
        source._di_source_id                                                                                              AS "_di_source_id",
        CAST(SUBSTRING(source.data ->> 'updatedAt' FROM '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z') AS DATE)           AS "updated_at",
        source.data ->> 'lieu_id'                                                                                         AS "lieu_id",
        services.data ->> 'serviceObjectId'                                                                               AS "id",
        NULLIF(services.data ->> 'name', '')                                                                              AS "name",
        services.data ->> 'category'                                                                                      AS "category",
        NULLIF(services.data ->> 'description', '')                                                                       AS "description",
        services.data -> 'hours'                                                                                          AS "hours",
        CAST(services.data ->> 'differentHours' AS BOOLEAN)                                                               AS "different_hours",
        CAST(services.data #>> '{close,actif}' AS BOOLEAN)                                                                AS "close__actif",
        CAST(SUBSTRING(services.data #>> '{close,dateDebut}' FROM '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z') AS DATE) AS "close__date_debut",
        CAST(SUBSTRING(services.data #>> '{close,dateFin}' FROM '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z') AS DATE)   AS "close__date_fin",
        CAST(services.data #>> '{modalities,inconditionnel}' AS BOOLEAN)                                                  AS "modalities__inconditionnel",
        CAST(services.data #>> '{modalities,appointment,checked}' AS BOOLEAN)                                             AS "modalities__appointment__checked",
        services.data #>> '{modalities,appointment,precisions}'                                                           AS "modalities__appointment__precisions",
        CAST(services.data #>> '{modalities,price,checked}' AS BOOLEAN)                                                   AS "modalities__price__checked",
        ARRAY_TO_STRING(
            ARRAY_REMOVE(
                ARRAY[services.data #>> '{modalities,price,precisions}', source.data #>> '{modalities,price,precisions}'],
                NULL
            ),
            E'\n\n'
        )                                                                                                                 AS "modalities__price__precisions",
        CAST(services.data #>> '{modalities,inscription,checked}' AS BOOLEAN)                                             AS "modalities__inscription__checked",
        services.data #>> '{modalities,inscription,precisions}'                                                           AS "modalities__inscription__precisions",
        CAST(services.data #>> '{modalities,orientation,checked}' AS BOOLEAN)                                             AS "modalities__orientation__checked",
        services.data #>> '{modalities,orientation,precisions}'                                                           AS "modalities__orientation__precisions",
        source.data -> 'sources'                                                                                          AS "sources"
    FROM
        source,
        LATERAL (SELECT services.* FROM JSONB_PATH_QUERY(source.data, '$.services_all[*]')) AS services (data)
),

final AS (
    SELECT *
    FROM services
    WHERE NOT sources @> '[{"name": "dora"}]'
)

SELECT * FROM final
