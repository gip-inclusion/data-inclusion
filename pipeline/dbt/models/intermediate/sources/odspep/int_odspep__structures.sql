WITH ressources_partenariales AS (
    SELECT * FROM {{ ref('int_odspep__enhanced_res_partenariales') }}
),

final AS (
    -- remove duplication introduced by us
    -- in the int_odspep__enhanced_res_partenariales model
    SELECT DISTINCT ON (1)
        id_res                                       AS "id",
        id_res                                       AS "adresse_id",
        CAST(NULL AS BOOLEAN)                        AS "antenne",
        NULL                                         AS "rna",
        'odspep'                                     AS "source",
        NULL                                         AS "horaires_ouverture",
        NULL                                         AS "accessibilite",
        CAST(NULL AS TEXT [])                        AS "labels_nationaux",
        CAST(NULL AS TEXT [])                        AS "labels_autres",
        CAST(NULL AS TEXT [])                        AS "thematiques",
        NULL                                         AS "typologie",
        date_derniere_modif                          AS "date_maj",
        NULL                                         AS "siret",
        RTRIM(SUBSTRING(nom_structure, 1, 150), '.') AS "nom",
        NULL                                         AS "lien_source",
        NULL                                         AS "presentation_resume",
        NULL                                         AS "presentation_detail",
        tel_1_ctc                                    AS "telephone",
        site_internet_ctc                            AS "site_web",
        mail_ctc                                     AS "courriel"
    FROM ressources_partenariales
    ORDER BY 1
)

SELECT * FROM final
