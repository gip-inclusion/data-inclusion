WITH source AS (
    SELECT *
    FROM {{ source('odspep', 'DD009_RES_PARTENARIALE') }}
),

final AS (
    SELECT
        "ID_RES"                                         AS "id",
        "ID_RES"                                         AS "id_res",
        "ID_CTC"                                         AS "id_ctc",
        "ID_PHY"                                         AS "id_phy",
        "ID_ADR"                                         AS "id_adr",
        "NOM_STRUCTURE_RSP"                              AS "nom_structure",
        "LIBELLE_COURT_RSP"                              AS "libelle_court",
        "SERVICE_RSP"                                    AS "service",
        "SERVICE_DESCRIPTION_RSP"                        AS "service_description",
        "MODALITE_ACCES_RSP"                             AS "modalite_acces",
        "RESPONSABLE_RSP"                                AS "responsable",
        "COMMENTAIRES_HORAIRE_RSP"                       AS "commentaires_horaire",
        "NUMERO_RSP"                                     AS "numero",
        "PRESCRIPTIBLE_RSP"                              AS "prescriptible",
        "TYPE_RES_PART_RSP"                              AS "type_res_part",
        "PROPRIETAIRE_RSP"                               AS "proprietaire",
        "CREATEUR_RSP"                                   AS "createur",
        "CODE_GROUPE_GDR"                                AS "code_groupe_gdr",
        "PERIMETRE_GEO_RSP"                              AS "perimetre_geo",
        "ARCHIVE_RSP"                                    AS "archive",
        "DIFFUSABLE_RSP"                                 AS "diffusable",
        TO_DATE("DATE_DEB_VALID_RSP", 'YYYY-MM-DD')      AS "date_deb_valid",
        TO_DATE("DATE_FIN_VALID_RSP", 'YYYY-MM-DD')      AS "date_fin_valid",
        TO_DATE("DATE_DERNIERE_MODIF_RSP", 'YYYY-MM-DD') AS "date_derniere_modif"
    FROM source
)

SELECT * FROM final
