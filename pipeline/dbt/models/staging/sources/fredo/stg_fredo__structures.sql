WITH source AS (
    {{ stg_source_header('fredo', 'structures') }}
),

final AS (
    SELECT
        data ->> 'id'                                                              AS "id",
        NULLIF(TRIM(data ->> 'adresse'), '')                                       AS "adresse",
        NULLIF(TRIM(data ->> 'code_postal'), '')                                   AS "code_postal",
        NULLIF(TRIM(data ->> 'commune'), '')                                       AS "commune",
        NULLIF(TRIM(data ->> 'frais'), '')                                         AS "frais",
        NULLIF(TRIM(data ->> 'horaires_ouverture'), '')                            AS "horaires_ouverture",
        TO_DATE(NULLIF(TRIM(data ->> 'last_update'), ''), 'YYYY-MM-DD HH24:MI:SS') AS "last_update",
        NULLIF(TRIM(data ->> 'lien_source'), '')                                   AS "site_web",
        NULLIF(TRIM(data ->> 'nom'), '')                                           AS "nom",
        NULLIF(TRIM(data ->> 'presentation_resume'), '')                           AS "presentation_resume",
        NULLIF(TRIM(data ->> 'siret'), '')                                         AS "siret",
        CAST(NULLIF(TRIM(data ->> 'latitude'), '') AS FLOAT)                       AS "latitude",
        CAST(NULLIF(TRIM(data ->> 'longitude'), '') AS FLOAT)                      AS "longitude",
        CASE
            WHEN data ->> 'telephone' IS NOT NULL
                THEN CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'telephone')) AS TEXT [])
            ELSE CAST(NULL AS TEXT [])
        END                                                                        AS "telephone",
        CASE
            WHEN data ->> 'email' IS NOT NULL
                THEN CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'email')) AS TEXT [])
            ELSE CAST(NULL AS TEXT [])
        END                                                                        AS "courriel",
        CASE
            WHEN data ->> 'categories' IS NOT NULL
                THEN CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'categories')) AS TEXT [])
            ELSE CAST(NULL AS TEXT [])
        END                                                                        AS "categories",
        CASE
            WHEN data ->> 'publics' IS NOT NULL
                THEN CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'publics')) AS TEXT [])
            ELSE CAST(NULL AS TEXT [])
        END                                                                        AS "publics",
        CASE
            WHEN data ->> 'quartiers' IS NOT NULL
                THEN CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'quartiers')) AS TEXT [])
            ELSE CAST(NULL AS TEXT [])
        END                                                                        AS "quartiers",
        CASE
            WHEN data ->> 'services' IS NOT NULL
                THEN CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'services')) AS TEXT [])
            ELSE CAST(NULL AS TEXT [])
        END                                                                        AS "services",
        CASE
            WHEN data ->> 'type_structure' IS NOT NULL
                THEN CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'type_structure')) AS TEXT [])
            ELSE CAST(NULL AS TEXT [])
        END                                                                        AS "type_structure"
    FROM source
)

SELECT * FROM final
