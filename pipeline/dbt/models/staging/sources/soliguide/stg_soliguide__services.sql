WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}
),

services AS (
    SELECT
        source._di_source_id                                                        AS "_di_source_id",
        CAST(source.data ->> 'updatedAt' AS DATE)                                   AS "updated_at",
        source.data ->> 'lieu_id'                                                   AS "lieu_id",
        inline_service.data ->> 'serviceObjectId'                                   AS "id",
        NULLIF(inline_service.data ->> 'name', '')                                  AS "name",
        inline_service.data ->> 'category'                                          AS "category",
        NULLIF(inline_service.data ->> 'description', '')                           AS "description",
        inline_service.data -> 'hours'                                              AS "hours",
        CAST(inline_service.data ->> 'differentHours' AS BOOLEAN)                   AS "different_hours",
        CAST(inline_service.data #>> '{close,actif}' AS BOOLEAN)                    AS "close__actif",
        CAST(inline_service.data #>> '{close,dateDebut}' AS DATE)                   AS "close__date_debut",
        CAST(inline_service.data #>> '{close,dateFin}' AS DATE)                     AS "close__date_fin",
        CAST(inline_service.data #>> '{modalities,inconditionnel}' AS BOOLEAN)      AS "modalities__inconditionnel",
        CAST(inline_service.data #>> '{modalities,appointment,checked}' AS BOOLEAN) AS "modalities__appointment__checked",
        inline_service.data #>> '{modalities,appointment,precisions}'               AS "modalities__appointment__precisions",
        CAST(inline_service.data #>> '{modalities,price,checked}' AS BOOLEAN)       AS "modalities__price__checked",
        CASE
            WHEN inline_service.data #>> '{modalities,price,precisions}' IS NULL THEN source.data #>> '{modalities,price,precisions}'
            WHEN source.data #>> '{modalities,price,precisions}' IS NULL THEN NULL
            ELSE CONCAT(inline_service.data #>> '{modalities,price,precisions}', ' ', source.data #>> '{modalities,price,precisions}')
        END                                                                         AS "modalities__price__precisions",
        CAST(inline_service.data #>> '{modalities,inscription,checked}' AS BOOLEAN) AS "modalities__inscription__checked",
        inline_service.data #>> '{modalities,inscription,precisions}'               AS "modalities__inscription__precisions",
        CAST(inline_service.data #>> '{modalities,orientation,checked}' AS BOOLEAN) AS "modalities__orientation__checked",
        inline_service.data #>> '{modalities,orientation,precisions}'               AS "modalities__orientation__precisions",
        source.data -> 'sources'                                                    AS "sources"
    FROM
        source,
        LATERAL (SELECT * FROM JSONB_PATH_QUERY(source.data, '$.services_all[*]')) AS inline_service (data)
),

final AS (
    SELECT *
    FROM services
    WHERE NOT sources @> '[{"name": "dora"}]'
)

SELECT * FROM final
