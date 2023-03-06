WITH ressources_partenariales AS (
    SELECT * FROM {{ ref('int_odspep__enhanced_res_partenariales') }}
),

final AS (
    -- remove duplication introduced by us
    -- in the int_odspep__enhanced_res_partenariales model
    SELECT DISTINCT ON (1)
        id_res                 AS "id",
        NULL                   AS "antenne",
        NULL                   AS "rna",
        longitude_adr          AS "longitude",
        latitude_adr           AS "latitude",
        'odspep'               AS "source",
        NULL                   AS "horaires_ouverture",
        NULL                   AS "accessibilite",
        NULL::TEXT[]           AS "labels_nationaux",
        NULL::TEXT[]           AS "labels_autres",
        NULL::TEXT[]           AS "thematiques",
        NULL                   AS "typologie",
        date_derniere_modif    AS "date_maj",
        NULL                   AS "siret",
        nom_structure          AS "nom",
        NULL                   AS "lien_source",
        l3_complement_adr      AS "complement_adresse",
        libelle_commune_adr    AS "commune",
        l4_numero_lib_voie_adr AS "adresse",
        NULL                   AS "presentation_resume",
        NULL                   AS "presentation_detail",
        tel_1_ctc              AS "telephone",
        site_internet_ctc      AS "site_web",
        code_postal_adr        AS "code_postal",
        code_commune_adr       AS "code_insee",
        mail_ctc               AS "courriel"
    FROM ressources_partenariales
    ORDER BY 1
)

SELECT * FROM final
