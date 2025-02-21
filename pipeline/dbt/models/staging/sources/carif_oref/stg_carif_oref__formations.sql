WITH source AS (
    SELECT * FROM {{ ref('_stg_carif_oref__source_filtered') }}
),

final AS (
    SELECT
        NULLIF(TRIM(source.data ->> '@numero'), '')            AS "numero",
        CAST((source.data ->> '@datemaj') AS DATE)             AS "date_maj",
        NULLIF(TRIM(source.data ->> 'intitule-formation'), '') AS "intitule_formation",
        NULLIF(TRIM(source.data ->> 'objectif-formation'), '') AS "objectif_formation",
        NULLIF(
            ARRAY_REMOVE(
                ARRAY(
                    SELECT codes.data ->> '$'
                    FROM JSONB_ARRAY_ELEMENTS(source.data -> 'domaine-formation' -> 'code-FORMACODE') AS codes (data)
                ),
                NULL
            ),
            '{}'
        )                                                      AS "domaine_formation__formacode"
    FROM source
)

SELECT * FROM final
