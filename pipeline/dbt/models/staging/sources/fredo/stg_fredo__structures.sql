WITH source AS (
    {{ stg_source_header('fredo', 'structures') }}
),

final AS (
    SELECT
        source.data ->> 'id'                                                              AS "id",
        NULLIF(TRIM(source.data ->> 'adresse'), '')                                       AS "adresse",
        NULLIF(TRIM(source.data ->> 'lien_source'), '')                                   AS "lien_source",
        NULLIF(TRIM(source.data ->> 'code_postal'), '')                                   AS "code_postal",
        NULLIF(TRIM(source.data ->> 'commune'), '')                                       AS "commune",
        NULLIF(TRIM(source.data ->> 'frais'), '')                                         AS "frais",
        NULLIF(TRIM(source.data ->> 'horaires_ouverture'), '')                            AS "horaires_ouverture",
        TO_DATE(NULLIF(TRIM(source.data ->> 'last_update'), ''), 'YYYY-MM-DD HH24:MI:SS') AS "last_update",
        NULLIF(TRIM(source.data ->> 'nom'), '')                                           AS "nom",
        NULLIF(TRIM(source.data ->> 'presentation_resume'), '')                           AS "presentation_resume",
        NULLIF(TRIM(source.data ->> 'siret'), '')                                         AS "siret",
        CAST(NULLIF(TRIM(source.data ->> 'latitude'), '') AS FLOAT)                       AS "latitude",
        CAST(NULLIF(TRIM(source.data ->> 'longitude'), '') AS FLOAT)                      AS "longitude"
    FROM source
)

SELECT * FROM final
