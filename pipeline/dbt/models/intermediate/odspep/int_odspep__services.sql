WITH ressources_partenariales AS (
    SELECT * FROM {{ ref('int_odspep__enhanced_res_partenariales') }}
),

di_thematique_by_odspep_type_res_part AS (
    SELECT x.*
    FROM (
        VALUES
        -- Mapping: https://www.notion.so/dora-beta/Explorer-ODSPEP-du-23-01-22-48e1d505c7cb4fdb9e3ec789449e7ca7#24743be5aac94306b6261e8e6c2f748c
        ('3', 'creation-dactivite'),
        ('5', 'handicap'),
        ('6', 'accompagnement-social-et-professionnel-personnalise'),
        ('8', 'mobilite'),
        ('9', 'numerique')
    ) AS x (type_res_part, thematique)
),

final AS (
    SELECT
        service                                         AS "nom",
        'odspep'                                        AS "source",
        NULL                                            AS "types",
        NULL                                            AS "prise_rdv",
        NULL::TEXT []                                   AS "frais",
        NULL                                            AS "frais_autres",
        NULL                                            AS "pre_requis",
        NULL                                            AS "cumulable",
        NULL                                            AS "justificatifs",
        NULL                                            AS "formulaire_en_ligne",
        NULL                                            AS "commune",
        NULL                                            AS "code_postal",
        NULL                                            AS "code_insee",
        NULL                                            AS "adresse",
        NULL                                            AS "complement_adresse",
        NULL                                            AS "longitude",
        NULL                                            AS "latitude",
        NULL                                            AS "recurrence",
        NULL                                            AS "date_creation",
        date_fin_valid                                  AS "date_suspension",
        NULL                                            AS "lien_source",
        NULL                                            AS "telephone",
        NULL                                            AS "courriel",
        NULL                                            AS "contact_public",
        date_derniere_modif                             AS "date_maj",
        NULL::TEXT []                                   AS "modes_accueil",
        zone_diffusion_code                             AS "zone_diffusion_code",
        zone_diffusion_type                             AS "zone_diffusion_type",
        zone_diffusion_libelle                          AS "zone_diffusion_nom",
        id_res                                          AS "structure_id",
        NULL::TEXT []                                   AS "profils",
        CONCAT(id_res, '_', zone_diffusion_unique_code) AS "id",
        ARRAY(
            SELECT di_thematique_by_odspep_type_res_part.thematique
            FROM di_thematique_by_odspep_type_res_part
            WHERE ressources_partenariales.type_res_part = di_thematique_by_odspep_type_res_part.type_res_part
        )::TEXT []                                      AS "thematiques",
        CASE LENGTH(service_description) <= 280
            WHEN TRUE THEN service_description
            WHEN FALSE THEN LEFT(service_description, 279) || '…'
        END                                             AS "presentation_resume",
        CASE LENGTH(service_description) <= 280
            WHEN TRUE THEN NULL
            WHEN FALSE THEN service_description
        END                                             AS "presentation_detail"
    FROM ressources_partenariales
    ORDER BY 1

)

SELECT * FROM final
