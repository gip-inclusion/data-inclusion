WITH source AS (
    {{ stg_source_header('data_inclusion_extra', 'structures') }}
),

final AS (
    SELECT
        _di_source_id                                                                                          AS "_di_source_id",
        CAST(data ->> 'antenne' AS BOOLEAN)                                                                    AS "antenne",
        TO_DATE(data ->> 'date_maj', 'DD/MM/YYYY')                                                             AS "date_maj",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'labels_autres.\d+'), NULL)    AS "labels_autres",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'labels_nationaux.\d+'), NULL) AS "labels_nationaux",
        CAST(data ->> 'latitude' AS FLOAT)                                                                     AS "latitude",
        CAST(data ->> 'longitude' AS FLOAT)                                                                    AS "longitude",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'thematiques.\d+'), NULL)      AS "thematiques",
        data ->> 'accessibilite'                                                                               AS "accessibilite",
        data ->> 'adresse'                                                                                     AS "adresse",
        data ->> 'code_insee'                                                                                  AS "code_insee",
        data ->> 'code_postal'                                                                                 AS "code_postal",
        data ->> 'commune'                                                                                     AS "commune",
        data ->> 'complement_adresse'                                                                          AS "complement_adresse",
        data ->> 'courriel'                                                                                    AS "courriel",
        data ->> 'horaires_ouverture'                                                                          AS "horaires_ouverture",
        data ->> 'id'                                                                                          AS "id",
        data ->> 'lien_source'                                                                                 AS "lien_source",
        data ->> 'nom'                                                                                         AS "nom",
        data ->> 'presentation_detail'                                                                         AS "presentation_detail",
        data ->> 'presentation_resume'                                                                         AS "presentation_resume",
        data ->> 'rna'                                                                                         AS "rna",
        data ->> 'siret'                                                                                       AS "siret",
        data ->> 'site_web'                                                                                    AS "site_web",
        data ->> 'source'                                                                                      AS "source",
        data ->> 'telephone'                                                                                   AS "telephone",
        data ->> 'typologie'                                                                                   AS "typologie"
    FROM source
    WHERE
        NOT COALESCE(CAST(data ->> '__ignore__' AS BOOLEAN), FALSE)
)

SELECT * FROM final
