WITH source AS (
    {{ stg_source_header('dora', 'structures') }}
),

final AS (
    SELECT
        _di_source_id                                                              AS "_di_source_id",
        CAST((data ->> 'antenne') AS BOOLEAN)                                      AS "antenne",
        CAST((data ->> 'date_maj') AS TIMESTAMP WITH TIME ZONE)                    AS "date_maj",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'labels_autres'))    AS "labels_autres",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'labels_nationaux')) AS "labels_nationaux",
        CAST((data ->> 'latitude') AS FLOAT)                                       AS "latitude",
        CAST((data ->> 'longitude') AS FLOAT)                                      AS "longitude",
        NULLIF(TRIM(data ->> 'accessibilite'), '')                                 AS "accessibilite",
        NULLIF(TRIM(data ->> 'adresse'), '')                                       AS "adresse",
        NULLIF(TRIM(data ->> 'code_insee'), '')                                    AS "code_insee",
        NULLIF(TRIM(data ->> 'code_postal'), '')                                   AS "code_postal",
        NULLIF(TRIM(data ->> 'commune'), '')                                       AS "commune",
        NULLIF(TRIM(data ->> 'complement_adresse'), '')                            AS "complement_adresse",
        NULLIF(TRIM(data ->> 'courriel'), '')                                      AS "courriel",
        NULLIF(TRIM(data ->> 'horaires_ouverture'), '')                            AS "horaires_ouverture",
        NULLIF(TRIM(data ->> 'id'), '')                                            AS "id",
        NULLIF(TRIM(data ->> 'lien_source'), '')                                   AS "lien_source",
        NULLIF(REGEXP_REPLACE(TRIM(data ->> 'nom'), '(?<!etc)\.+$', ''), '')       AS "nom",
        NULLIF(TRIM(data ->> 'presentation_detail'), '')                           AS "presentation_detail",
        NULLIF(TRIM(data ->> 'presentation_resume'), '')                           AS "presentation_resume",
        NULLIF(TRIM(data ->> 'rna'), '')                                           AS "rna",
        NULLIF(TRIM(data ->> 'siret'), '')                                         AS "siret",
        NULLIF(TRIM(data ->> 'site_web'), '')                                      AS "site_web",
        NULLIF(TRIM(data ->> 'source'), '')                                        AS "source",
        NULLIF(TRIM(data ->> 'telephone'), '')                                     AS "telephone",
        NULLIF(TRIM(data ->> 'typologie'), '')                                     AS "typologie"
    FROM source
)

SELECT * FROM final
