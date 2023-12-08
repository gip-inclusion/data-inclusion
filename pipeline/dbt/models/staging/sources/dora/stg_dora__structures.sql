WITH source AS (
    {{ stg_source_header('dora', 'structures') }}
),

final AS (
    SELECT
        _di_source_id                                                                       AS "_di_source_id",
        (data ->> 'antenne')::BOOLEAN                                                       AS "antenne",
        (data ->> 'date_maj')::TIMESTAMP WITH TIME ZONE                                     AS "date_maj",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'labels_autres'))::TEXT []    AS "labels_autres",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'labels_nationaux'))::TEXT [] AS "labels_nationaux",
        (data ->> 'latitude')::FLOAT                                                        AS "latitude",
        (data ->> 'longitude')::FLOAT                                                       AS "longitude",
        (data ->> 'thematiques')::TEXT []                                                   AS "thematiques",
        data ->> 'accessibilite'                                                            AS "accessibilite",
        data ->> 'adresse'                                                                  AS "adresse",
        data ->> 'code_insee'                                                               AS "code_insee",
        data ->> 'code_postal'                                                              AS "code_postal",
        data ->> 'commune'                                                                  AS "commune",
        NULLIF(TRIM(data ->> 'complement_adresse'), '')                                     AS "complement_adresse",
        NULLIF(TRIM(data ->> 'courriel'), '')                                               AS "courriel",
        data ->> 'horaires_ouverture'                                                       AS "horaires_ouverture",
        data ->> 'id'                                                                       AS "id",
        data ->> 'lien_source'                                                              AS "lien_source",
        NULLIF(TRIM(data ->> 'nom'), '')                                                    AS "nom",
        data ->> 'presentation_detail'                                                      AS "presentation_detail",
        data ->> 'presentation_resume'                                                      AS "presentation_resume",
        data ->> 'rna'                                                                      AS "rna",
        data ->> 'siret'                                                                    AS "siret",
        data ->> 'site_web'                                                                 AS "site_web",
        data ->> 'source'                                                                   AS "source",
        data ->> 'telephone'                                                                AS "telephone",
        data ->> 'typologie'                                                                AS "typologie"
    FROM source
)

SELECT * FROM final
