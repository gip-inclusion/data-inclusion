WITH source AS (
    SELECT * FROM {{ source('agefiph', 'structures') }}
),

final AS (
    SELECT
        _di_source_id                  AS "_di_source_id",
        data ->> 'id'                  AS "id",
        data ->> 'courriel'            AS "courriel",
        data ->> 'nom'                 AS "nom",
        data ->> 'commune'             AS "commune",
        data ->> 'code_postal'         AS "code_postal",
        data ->> 'code_insee'          AS "code_insee",
        data ->> 'adresse'             AS "adresse",
        data ->> 'complement_adresse'  AS "complement_adresse",
        data ->> 'longitude'           AS "longitude",
        data ->> 'latitude'            AS "latitude",
        data ->> 'typologie'           AS "typologie",
        data ->> 'telephone'           AS "telephone",
        data ->> 'site_web'            AS "site_web",
        data ->> 'presentation_resume' AS "presentation_resume",
        data ->> 'presentation_detail' AS "presentation_detail",
        data ->> 'source'              AS "source",
        data ->> 'date_maj'            AS "date_maj",
        data ->> 'antenne'             AS "antenne",
        data ->> 'lien_source'         AS "lien_source",
        data ->> 'horaires_ouverture'  AS "horaires_ouverture",
        data ->> 'accessibilite'       AS "accessibilite",
        data ->> 'labels_nationaux'    AS "labels_nationaux",
        data ->> 'labels_autres'       AS "labels_autres",
        data ->> 'thematiques'         AS "thematiques"
    FROM source
)

SELECT * FROM final
