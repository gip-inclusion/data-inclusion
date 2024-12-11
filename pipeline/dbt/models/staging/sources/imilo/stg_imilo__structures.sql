WITH source AS (
    {{ stg_source_header('imilo', 'structures') }}
),

final AS (
    SELECT
        _di_source_id                                                             AS "_di_source_id",
        NULLIF(TRIM(data -> 'structures' ->> 'id_structure'), '')                 AS "id_structure",
        NULLIF(TRIM(data -> 'structures' ->> 'email'), '')                        AS "email",
        NULLIF(TRIM(data -> 'structures' ->> 'commune'), '')                      AS "commune",
        NULLIF(TRIM(data -> 'structures' ->> 'siret'), '')                        AS "siret",
        NULLIF(TRIM(data -> 'structures' ->> 'horaires'), '')                     AS "horaires",
        NULLIF(TRIM(data -> 'structures' ->> 'site_web'), '')                     AS "site_web",
        NULLIF(TRIM(data -> 'structures' ->> 'telephone'), '')                    AS "telephone",
        NULLIF(TRIM(data -> 'structures' ->> 'typologie'), '')                    AS "typologie",
        NULLIF(TRIM(data -> 'structures' ->> 'code_insee'), '')                   AS "code_insee",
        NULLIF(TRIM(data -> 'structures' ->> 'code_postal'), '')                  AS "code_postal",
        NULLIF(TRIM(data -> 'structures' ->> 'nom_structure'), '')                AS "nom_structure",
        NULLIF(TRIM(data -> 'structures' ->> 'labels_nationaux'), '')             AS "labels_nationaux",
        NULLIF(TRIM(data -> 'structures' ->> 'adresse_structure'), '')            AS "adresse_structure",
        NULLIF(TRIM(data -> 'structures' ->> 'presentation'), '')                 AS "presentation",
        NULLIF(TRIM(data -> 'structures' ->> 'complement_adresse_structure'), '') AS "complement_adresse_structure",
        CAST((data -> 'structures' ->> 'date_maj') AS TIMESTAMP WITH TIME ZONE)   AS "date_maj"
    FROM source
)

SELECT * FROM final
