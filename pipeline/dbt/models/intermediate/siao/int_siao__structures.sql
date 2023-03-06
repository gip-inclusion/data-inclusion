WITH etablissements AS (
    SELECT * FROM {{ ref('stg_siao__etablissements') }}
),

final AS (
    SELECT
        id                  AS "id",
        code_siret          AS "siret",
        NULL::BOOLEAN       AS "antenne",
        NULL                AS "rna",
        ville               AS "commune",
        code_postal         AS "code_postal",
        NULL                AS "code_insee",
        adresse             AS "adresse",
        NULL                AS "complement_adresse",
        NULL::FLOAT         AS "longitude",
        NULL::FLOAT         AS "latitude",
        telephone           AS "telephone",
        mail                AS "courriel",
        NULL                AS "site_web",
        _di_source_id       AS "source",
        NULL                AS "lien_source",
        NULL                AS "horaires_ouverture",
        NULL                AS "accessibilite",
        NULL::TEXT[]        AS "labels_nationaux",
        NULL::TEXT[]        AS "labels_autres",
        NULL::TEXT[]        AS "thematiques",
        NULL                AS "typologie",
        NULL                AS "presentation_resume",
        NULL                AS "presentation_detail",
        NULL::DATE          AS "date_maj",
        nom_de_la_structure AS "nom"
    FROM etablissements
)

SELECT * FROM final
