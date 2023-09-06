WITH source AS (
    SELECT * FROM {{ source('reseau_alpha', 'structures_services') }}
),

final AS (
    SELECT
        _di_source_id           AS "_di_source_id",
        data ->> 'id'           AS "id",
        data ->> 'structure_id' AS "structure_id"
    -- data ->> 'courriel'                 AS "courriel",
    -- data ->> 'nom'                      AS "nom",
    -- data ->> 'commune'                  AS "commune",
    -- data ->> 'code_postal'              AS "code_postal",
    -- data ->> 'code_insee'               AS "code_insee",
    -- data ->> 'adresse'                  AS "adresse",
    -- data ->> 'complement_adresse'       AS "complement_adresse",
    -- CAST(data ->> 'longitude' AS FLOAT) AS "longitude",
    -- CAST(data ->> 'latitude' AS FLOAT)  AS "latitude",
    -- data ->> 'typologie'                AS "typologie",git 
    -- data ->> 'telephone'                AS "telephone",
    -- data ->> 'site_web'                 AS "site_web",
    -- data ->> 'presentation_resume'      AS "presentation_resume",
    -- data ->> 'presentation_detail'      AS "presentation_detail",
    -- data ->> 'source'                   AS "source",
    -- CAST(data ->> 'date_maj' AS DATE)   AS "date_maj",
    -- CAST(data ->> 'antenne' AS BOOLEAN) AS "antenne",
    -- data ->> 'lien_source'              AS "lien_source",
    -- data ->> 'horaires_ouverture'       AS "horaires_ouverture",
    -- data ->> 'accessibilite'            AS "accessibilite",
    -- ARRAY[data ->> 'labels_nationaux']  AS "labels_nationaux"
    FROM source
)

SELECT * FROM final
