WITH source AS (
    {{ stg_source_header('imilo', 'structures') }}
),

final AS (
    SELECT
        _di_source_id                                                             AS "_di_source_id",
        CAST(data -> 'structures' ->> 'id_structure' AS TEXT)                     AS "id",
        NULLIF(TRIM(data -> 'structures' ->> 'email'), '')                        AS "courriel",
        NULLIF(TRIM(data -> 'structures' ->> 'commune'), '')                      AS "commune",
        NULLIF(TRIM(data -> 'structures' ->> 'siret'), '')                        AS "siret",
        NULLIF(TRIM(data -> 'structures' ->> 'horaires'), '')                     AS "horaires_ouverture",
        NULLIF(TRIM(data -> 'structures' ->> 'site_web'), '')                     AS "site_web",
        NULLIF(TRIM(data -> 'structures' ->> 'telephone'), '')                    AS "telephone",
        NULLIF(TRIM(data -> 'structures' ->> 'typologie'), '')                    AS "typologie",
        NULLIF(TRIM(data -> 'structures' ->> 'code_insee'), '')                   AS "code_insee",
        NULLIF(TRIM(data -> 'structures' ->> 'code_postal'), '')                  AS "code_postal",
        NULLIF(TRIM(data -> 'structures' ->> 'nom_structure'), '')                AS "nom",
        NULLIF(TRIM(data -> 'structures' ->> 'labels_nationaux'), '')             AS "labels_nationaux",
        NULLIF(TRIM(data -> 'structures' ->> 'adresse_structure'), '')            AS "adresse",
        NULLIF(TRIM(data -> 'structures' ->> 'presentation_resumee'), '')         AS "presentation_resume",
        NULLIF(TRIM(data -> 'structures' ->> 'presentation_detaillee'), '')       AS "presentation_detail",
        NULLIF(TRIM(data -> 'structures' ->> 'complement_adresse_structure'), '') AS "complement_adresse",
        CAST((data -> 'structures' ->> 'date_maj') AS TIMESTAMP WITH TIME ZONE)   AS "date_maj"
    FROM source
)

SELECT * FROM final