WITH source AS (
    {{ stg_source_header('pole_emploi', 'structures') }}
),

final AS (
    SELECT
        _di_source_id                                                                               AS "_di_source_id",
        CAST((data ->> 'antenne') AS BOOLEAN)                                                       AS "antenne",
        CAST((data ->> 'date_maj') AS TIMESTAMP WITH TIME ZONE)                                     AS "date_maj",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'labels_autres')) AS TEXT [])    AS "labels_autres",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'labels_nationaux')) AS TEXT []) AS "labels_nationaux",
        CAST((data ->> 'latitude') AS FLOAT)                                                        AS "latitude",
        CAST((data ->> 'longitude') AS FLOAT)                                                       AS "longitude",
        CAST((data ->> 'thematiques') AS TEXT [])                                                   AS "thematiques",
        data ->> 'accessibilite'                                                                    AS "accessibilite",
        data ->> 'adresse'                                                                          AS "adresse",
        data ->> 'code_insee'                                                                       AS "code_insee",
        data ->> 'code_postal'                                                                      AS "code_postal",
        data ->> 'commune'                                                                          AS "commune",
        NULLIF(TRIM(data ->> 'complement_adresse'), '')                                             AS "complement_adresse",
        data ->> 'courriel'                                                                         AS "courriel",
        data ->> 'horaires_ouverture'                                                               AS "horaires_ouverture",
        data ->> 'id'                                                                               AS "id",
        data ->> 'lien_source'                                                                      AS "lien_source",
        NULLIF(TRIM(data ->> 'nom'), '')                                                            AS "nom",
        data ->> 'presentation_detail'                                                              AS "presentation_detail",
        data ->> 'presentation_resume'                                                              AS "presentation_resume",
        data ->> 'rna'                                                                              AS "rna",
        data ->> 'siret'                                                                            AS "siret",
        data ->> 'site_web'                                                                         AS "site_web",
        data ->> 'source'                                                                           AS "source",
        data ->> 'telephone'                                                                        AS "telephone",
        data ->> 'typologie'                                                                        AS "typologie"
    FROM source
    -- select PE structure(s)
    WHERE data ->> 'id' = 'f26d4cc9-6ca8-4864-ad1c-013c38ab7cfb'
)

SELECT * FROM final
