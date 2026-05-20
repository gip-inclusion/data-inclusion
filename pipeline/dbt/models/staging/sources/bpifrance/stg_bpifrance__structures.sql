WITH source AS (
    {{ stg_source_header('bpifrance', 'structures') }}
),

final AS (
    SELECT
        NULLIF(TRIM(data ->> 'adresse'), '')                    AS "adresse",
        NULLIF(TRIM(data ->> 'code_insee'), '')                 AS "code_insee",
        NULLIF(TRIM(data ->> 'code_postal'), '')                AS "code_postal",
        NULLIF(TRIM(data ->> 'commune'), '')                    AS "commune",
        NULLIF(TRIM(data ->> 'complement_adresse'), '')         AS "complement_adresse",
        NULLIF(TRIM(data ->> 'courriel'), '')                   AS "courriel",
        CAST((data ->> 'date_maj') AS TIMESTAMP WITH TIME ZONE) AS "date_maj",
        NULLIF(TRIM(data ->> 'description'), '')                AS "description",
        NULLIF(TRIM(data ->> 'id'), '')                         AS "id",
        CAST((data ->> 'latitude') AS FLOAT)                    AS "latitude",
        CAST((data ->> 'longitude') AS FLOAT)                   AS "longitude",
        NULLIF(TRIM(data ->> 'lien_source'), '')                AS "lien_source",
        NULLIF(TRIM(data ->> 'nom'), '')                        AS "nom",
        NULLIF(TRIM(data ->> 'site_web'), '')                   AS "site_web",
        NULLIF(TRIM(data ->> 'source'), '')                     AS "source",
        NULLIF(TRIM(data ->> 'telephone'), '')                  AS "telephone"
    FROM source
)

SELECT * FROM final
