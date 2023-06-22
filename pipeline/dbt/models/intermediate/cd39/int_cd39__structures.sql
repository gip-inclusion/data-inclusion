WITH structures_services AS (
    SELECT * FROM {{ ref('stg_cd39__structures_services') }}
),

final AS (
    SELECT
        structure_id       AS "id",
        NULL               AS "siret",
        NULL::BOOLEAN      AS "antenne",
        NULL               AS "rna",
        nom_structure      AS "nom",
        commune            AS "commune",
        code_postal        AS "code_postal",
        NULL               AS "code_insee",
        adresse            AS "adresse",
        NULL               AS "complement_adresse",
        NULL               AS "longitude",
        NULL               AS "latitude",
        telephone          AS "telephone",
        courriel_structure AS "courriel",
        site_web           AS "site_web",
        _di_source_id      AS "source",
        NULL               AS "lien_source",
        NULL               AS "horaires_ouverture",
        NULL               AS "accessibilite",
        NULL::TEXT []      AS "labels_nationaux",
        NULL::TEXT []      AS "labels_autres",
        NULL::TEXT []      AS "thematiques",
        typologie          AS "typologie",
        NULL               AS "presentation_resume",
        NULL               AS "presentation_detail",
        NULL               AS "date_maj"
    FROM structures_services
)

SELECT * FROM final
