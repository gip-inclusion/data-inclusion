WITH source AS (
    {{ stg_source_header('emplois_de_linclusion', 'organisations') }}),

final AS (
    SELECT
        source.data ->> 'id'                                               AS "id",
        NULLIF(TRIM(source.data ->> 'nom'), '')                            AS "nom",
        CAST(source.data ->> 'date_maj' AS DATE)                           AS "date_maj",
        NULLIF(TRIM(source.data ->> 'kind'), '')                           AS "kind",
        NULLIF(TRIM(source.data ->> 'commune'), '')                        AS "commune",
        NULLIF(NULLIF(TRIM(source.data ->> 'siret'), ''), REPEAT('0', 14)) AS "siret",
        NULLIF(TRIM(source.data ->> 'description'), '')                    AS "description",
        NULLIF(TRIM(source.data ->> 'adresse'), '')                        AS "adresse",
        NULLIF(TRIM(source.data ->> 'complement_adresse'), '')             AS "complement_adresse",
        NULLIF(TRIM(source.data ->> 'code_postal'), '')                    AS "code_postal",
        NULLIF(TRIM(source.data ->> 'site_web'), '')                       AS "site_web",
        NULLIF(CAST(source.data ->> 'longitude' AS FLOAT), 0)              AS "longitude",
        NULLIF(CAST(source.data ->> 'latitude' AS FLOAT), 0)               AS "latitude",
        NULLIF(TRIM(source.data ->> 'lien_source'), '')                    AS "lien_source",
        NULLIF(TRIM(source.data ->> 'telephone'), '')                      AS "telephone",
        NULLIF(TRIM(source.data ->> 'courriel'), '')                       AS "courriel"
    FROM source
)

SELECT * FROM final
