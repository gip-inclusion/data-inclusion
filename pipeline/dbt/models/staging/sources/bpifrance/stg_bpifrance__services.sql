WITH source AS (
    {{ stg_source_header('bpifrance', 'services') }}),

final AS (
    SELECT DISTINCT ON (1)-- TODO: remove DISTINCT ON when the source data will be cleaned and deduplicated

        NULLIF(TRIM(data ->> 'id'), '')                                                                     AS "id",
        CAST((data ->> 'date_maj') AS TIMESTAMP WITH TIME ZONE)                                             AS "date_maj",
        CAST((data ->> 'latitude') AS FLOAT)                                                                AS "latitude",
        CAST((data ->> 'longitude') AS FLOAT)                                                               AS "longitude",
        NULLIF(TRIM(data ->> 'adresse'), '')                                                                AS "adresse",
        NULLIF(TRIM(data ->> 'code_insee'), '')                                                             AS "code_insee",
        NULLIF(TRIM(data ->> 'code_postal'), '')                                                            AS "code_postal",
        NULLIF(TRIM(data ->> 'commune'), '')                                                                AS "commune",
        NULLIF(TRIM(data ->> 'complement_adresse'), '')                                                     AS "complement_adresse",
        NULLIF(TRIM(data ->> 'courriel'), '')                                                               AS "courriel",
        NULLIF(TRIM(data ->> 'lien_source'), '')                                                            AS "lien_source",
        NULLIF(TRIM(data ->> 'lien_mobilisation'), '')                                                      AS "lien_mobilisation",
        NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(data ->> 'nom'), '\.{2,}$', '…'), '(?<!etc)\.$', ''), '') AS "nom",
        NULLIF(TRIM(data ->> 'description'), '')                                                            AS "description",
        NULLIF(TRIM(data ->> 'source'), '')                                                                 AS "source",
        NULLIF(TRIM(data ->> 'structure_id'), '')                                                           AS "structure_id",
        NULLIF(TRIM(data ->> 'telephone'), '')                                                              AS "telephone",
        NULLIF(TRIM(data ->> 'type'), '')                                                                   AS "type"
    FROM source
)

SELECT * FROM final
