{% set source_model = source('soliguide', 'lieux') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    ),

{% else %}

WITH source AS (
    SELECT
        NULL                AS "_di_source_id",
        CAST(NULL AS JSONB) AS "data"
    WHERE FALSE
),

{% endif %}

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
