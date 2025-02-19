WITH source AS (
    {{ stg_source_header('carif_oref', 'formations') }}
),

final AS (
    SELECT
        NULLIF(TRIM(data ->> '@numero'), '')                         AS "numero",
        CAST((data ->> '@datemaj') AS DATE)                          AS "date_maj",
        NULLIF(TRIM(data ->> 'intitule-formation'), '')              AS "intitule_formation",
        NULLIF(TRIM(data ->> 'objectif-formation'), '')              AS "objectif_formation",
        NULLIF(TRIM(data ->> 'domaine-formation'), '')               AS "domaine_formation"
    FROM source
)

SELECT * FROM final
