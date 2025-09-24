WITH ressources_partenariales AS (
    SELECT * FROM {{ ref('int_odspep__enhanced_res_partenariales') }}
),

final AS (
    -- remove duplication introduced by us
    -- in the int_odspep__enhanced_res_partenariales model
    SELECT DISTINCT ON (1)
        id_res              AS "id",
        id_res              AS "adresse_id",
        NULL                AS "rna",
        'odspep'            AS "source",
        NULL                AS "horaires_ouverture",
        NULL                AS "accessibilite",
        NULL::TEXT []       AS "labels_nationaux",
        NULL::TEXT []       AS "labels_autres",
        NULL::TEXT []       AS "thematiques",
        NULL                AS "typologie",
        date_derniere_modif AS "date_maj",
        NULL                AS "siret",
        nom_structure       AS "nom",
        NULL                AS "lien_source",
        NULL                AS "presentation_resume",
        NULL                AS "presentation_detail",
        tel_1_ctc           AS "telephone",
        site_internet_ctc   AS "site_web",
        mail_ctc            AS "courriel"
    FROM ressources_partenariales
    ORDER BY 1
)

SELECT * FROM final
