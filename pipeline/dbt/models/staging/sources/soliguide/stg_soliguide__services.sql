WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}
),

final AS (
    SELECT
        source._di_source_id                                                  AS "_di_source_id",
        CAST(source.data ->> 'updatedAt' AS DATE)                             AS "updated_at",
        source.data ->> 'lieu_id'                                             AS "lieu_id",
        services.data ->> 'serviceObjectId'                                   AS "id",
        NULLIF(services.data ->> 'name', '')                                  AS "name",
        services.data ->> 'categorie'                                         AS "categorie",
        NULLIF(services.data ->> 'description', '')                           AS "description",
        services.data -> 'hours'                                              AS "hours",
        CAST(services.data ->> 'differentHours' AS BOOLEAN)                   AS "different_hours",
        CAST(services.data #>> '{close,actif}' AS BOOLEAN)                    AS "close__actif",
        CAST(services.data #>> '{close,dateDebut}' AS DATE)                   AS "close__date_debut",
        CAST(services.data #>> '{close,dateFin}' AS DATE)                     AS "close__date_fin",
        CAST(services.data #>> '{modalities,inconditionnel}' AS BOOLEAN)      AS "modalities__inconditionnel",
        CAST(services.data #>> '{modalities,appointment,checked}' AS BOOLEAN) AS "modalities__appointment__checked",
        services.data #>> '{modalities,appointment,precisions}'               AS "modalities__appointment__precisions",
        CAST(services.data #>> '{modalities,inscription,checked}' AS BOOLEAN) AS "modalities__inscription__checked",
        services.data #>> '{modalities,inscription,precisions}'               AS "modalities__inscription__precisions",
        CAST(services.data #>> '{modalities,orientation,checked}' AS BOOLEAN) AS "modalities__orientation__checked",
        services.data #>> '{modalities,orientation,precisions}'               AS "modalities__orientation__precisions"
    FROM
        source,
        LATERAL(SELECT * FROM JSONB_PATH_QUERY(source.data, '$.services_all[*]')) AS services (data)
)

SELECT * FROM final
