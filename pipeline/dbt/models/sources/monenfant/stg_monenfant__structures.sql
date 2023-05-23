WITH source AS (
    SELECT * FROM {{ source('monenfant', 'structures') }}
),

final AS (
    SELECT
        _di_source_id                                     AS "_di_source_id",
        NULL::TEXT []                                     AS "thematiques",
        NULL::TEXT []                                     AS "labels_nationaux",
        (data ->> 'antenne')::BOOLEAN                     AS "antenne",
        (REPLACE(data ->> 'longitude', ',', '.'))::FLOAT  AS "longitude",
        (REPLACE(data ->> 'latitude', ',', '.'))::FLOAT   AS "latitude",
        ((data ->> 'date_maj'))::TIMESTAMP WITH TIME ZONE AS "date_maj",
        data ->> 'id'                                     AS "id",
        data ->> 'nom'                                    AS "nom",
        data ->> 'siret'                                  AS "siret",
        data ->> 'rna'                                    AS "rna",
        data ->> 'source'                                 AS "source",
        data ->> 'adresse'                                AS "adresse",
        data ->> 'complement_adresse'                     AS "complement_adresse",
        data ->> 'commune'                                AS "commune",
        data ->> 'courriel'                               AS "courriel",
        data ->> 'site_web'                               AS "site_web",
        data ->> 'telephone'                              AS "telephone",
        data ->> 'code_postal'                            AS "code_postal",
        data ->> 'code_insee'                             AS "code_insee",
        data ->> 'horaires_ouverture'                     AS "horaires_ouverture",
        data ->> 'lien_source'                            AS "lien_source",
        data ->> 'accessibilite'                          AS "accessibilite",
        data ->> 'labels_autres'                          AS "labels_autres",
        data ->> 'typologie'                              AS "typologie",
        data ->> 'presentation_resume'                    AS "presentation_resume",
        data ->> 'presentation_detail'                    AS "presentation_detail"
    FROM source
)

SELECT * FROM final
