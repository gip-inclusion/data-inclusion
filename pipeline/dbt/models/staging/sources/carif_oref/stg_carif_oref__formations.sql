WITH source AS (
    SELECT * FROM {{ ref('_stg_carif_oref__source_filtered') }}
),

final AS (
    SELECT
        NULLIF(TRIM(source.data ->> '@numero'), '')                                      AS "numero",
        CAST((source.data ->> '@datemaj') AS DATE)                                       AS "date_maj",
        NULLIF(RTRIM(TRIM(source.data ->> 'intitule-formation'), '.'), '')               AS "intitule_formation",
        NULLIF(NULLIF(TRIM(source.data ->> 'objectif-formation'), ''), 'Non renseigné') AS "objectif_formation",
        NULLIF(TRIM(source.data ->> 'contenu-formation'), '')                            AS "contenu_formation",
        NULLIF(
            ARRAY_REMOVE(
                ARRAY(
                    SELECT codes.data ->> '$'
                    FROM JSONB_ARRAY_ELEMENTS(source.data -> 'domaine-formation' -> 'code-FORMACODE') AS codes (data)
                ),
                NULL
            ),
            '{}'
        )                                                                                AS "domaine_formation__formacode",
        NULLIF(
            ARRAY_REMOVE(
                ARRAY(
                    SELECT NULLIF(TRIM(x.urlweb), '')
                    FROM JSONB_ARRAY_ELEMENTS_TEXT(source.data -> 'url-formation' -> 'urlweb') AS x (urlweb)
                ),
                NULL
            ),
            '{}'
        )                                                                                AS "url_formation",
        JSONB_BUILD_OBJECT(
            'code-niveau-entree', source.data -> 'code-niveau-entree',
            'code-niveau-sortie', source.data -> 'code-niveau-sortie'
        )                                                                                AS "raw"
    FROM source
)

SELECT * FROM final
