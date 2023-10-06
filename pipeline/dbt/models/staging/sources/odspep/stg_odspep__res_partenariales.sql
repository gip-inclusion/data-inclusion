{% set source_model = source('odspep', 'DD009_RES_PARTENARIALE') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    ),

    ressources_partenariales AS (
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
            "PRESCRIPTIBLE_RSP"::BOOLEAN                     AS "prescriptible",
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
    ),

    final AS (
        SELECT *
        FROM ressources_partenariales
        WHERE date_derniere_modif IS NOT NULL AND EXTRACT(YEAR FROM date_derniere_modif) >= 2021
    )

    SELECT * FROM final

{% else %}

SELECT
    NULL          AS "id",
    NULL          AS "id_res",
    NULL          AS "id_ctc",
    NULL          AS "id_phy",
    NULL          AS "id_adr",
    NULL          AS "nom_structure",
    NULL          AS "libelle_court",
    NULL          AS "service",
    NULL          AS "service_description",
    NULL          AS "modalite_acces",
    NULL          AS "responsable",
    NULL          AS "commentaires_horaire",
    NULL          AS "numero",
    NULL::BOOLEAN AS "prescriptible",
    NULL          AS "type_res_part",
    NULL          AS "proprietaire",
    NULL          AS "createur",
    NULL          AS "code_groupe_gdr",
    NULL          AS "perimetre_geo",
    NULL          AS "archive",
    NULL          AS "diffusable",
    NULL::DATE    AS "date_deb_valid",
    NULL::DATE    AS "date_fin_valid",
    NULL::DATE    AS "date_derniere_modif"
WHERE FALSE

{% endif %}
