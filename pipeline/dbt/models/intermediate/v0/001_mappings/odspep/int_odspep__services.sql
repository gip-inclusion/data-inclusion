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
        id_res                                          AS "adresse_id",
        service                                         AS "nom",
        'odspep'                                        AS "source",
        CAST(NULL AS TEXT [])                           AS "types",
        NULL                                            AS "prise_rdv",
        NULL                                            AS "lien_mobilisation",
        CAST(NULL AS TEXT [])                           AS "frais",
        NULL                                            AS "frais_autres",
        CAST(NULL AS TEXT [])                           AS "pre_requis",
        CAST(NULL AS TEXT [])                           AS "justificatifs",
        NULL                                            AS "conditions_acces",
        NULL                                            AS "formulaire_en_ligne",
        NULL                                            AS "recurrence",
        NULL                                            AS "lien_source",
        NULL                                            AS "telephone",
        NULL                                            AS "courriel",
        NULL                                            AS "contact_nom_prenom",
        date_derniere_modif                             AS "date_maj",
        CAST(NULL AS TEXT [])                           AS "modes_accueil",
        CAST(NULL AS TEXT [])                           AS "modes_orientation_accompagnateur",
        NULL                                            AS "modes_orientation_accompagnateur_autres",
        CAST(NULL AS TEXT [])                           AS "modes_orientation_beneficiaire",
        NULL                                            AS "modes_orientation_beneficiaire_autres",
        CAST(NULL AS TEXT [])                           AS "modes_mobilisation",
        CAST(NULL AS TEXT [])                           AS "mobilisable_par",
        NULL                                            AS "mobilisation_precisions",
        zone_diffusion_code                             AS "zone_diffusion_code",
        zone_diffusion_type                             AS "zone_diffusion_type",
        zone_diffusion_libelle                          AS "zone_diffusion_nom",
        CAST(NULL AS FLOAT)                             AS "volume_horaire_hebdomadaire",
        CAST(NULL AS INT)                               AS "nombre_semaines",
        id_res                                          AS "structure_id",
        CAST(NULL AS TEXT [])                           AS "profils",
        NULL                                            AS "profils_precisions",
        CAST(ARRAY(
            SELECT di_thematique_by_odspep_type_res_part.thematique
            FROM di_thematique_by_odspep_type_res_part
            WHERE ressources_partenariales.type_res_part = di_thematique_by_odspep_type_res_part.type_res_part
        ) AS TEXT [])                                   AS "thematiques",
        CONCAT(id_res, '_', zone_diffusion_unique_code) AS "id",
        CASE
            WHEN LENGTH(service_description) <= 280 THEN service_description
            ELSE LEFT(service_description, 279) || '…'
        END                                             AS "presentation_resume",
        CASE
            WHEN LENGTH(service_description) <= 280 THEN NULL
            ELSE service_description
        END                                             AS "presentation_detail",
        NULL                                            AS "page_web"
    FROM ressources_partenariales
    ORDER BY 1

)

SELECT * FROM final
