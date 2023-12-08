WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}
),

final AS (
    SELECT
        source._di_source_id                                AS "_di_source_id",
        CAST(source.data ->> 'updatedAt' AS DATE)           AS "updated_at",
        source.data ->> 'lieu_id'                           AS "lieu_id",
        services.data ->> 'serviceObjectId'                 AS "id",
        NULLIF(services.data ->> 'name', '')                AS "name",
        services.data ->> 'categorie'                       AS "categorie",
        NULLIF(services.data ->> 'description', '')         AS "description",
        services.data -> 'hours'                            AS "hours",
        CAST(services.data ->> 'differentHours' AS BOOLEAN) AS "different_hours",
        CAST(services.data #>> '{close,actif}' AS BOOLEAN)  AS "close__actif",
        CAST(services.data #>> '{close,dateDebut}' AS DATE) AS "close__date_debut",
        CAST(services.data #>> '{close,dateFin}' AS DATE)   AS "close__date_fin"
    FROM
        source,
        LATERAL(SELECT * FROM JSONB_PATH_QUERY(source.data, '$.services_all[*]')) AS services (data)
)

SELECT * FROM final
