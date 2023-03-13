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
    ) AS x(type_res_part, thematique)
),

final AS (
    SELECT
        service                                         AS "nom",
        'odspep'                                        AS "source",
        service_description                             AS "presentation_resume",
        NULL                                            AS "presentation_detail",
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
        NULL::TEXT[]                                    AS "modes_accueil",
        zone_diffusion_code                             AS "zone_diffusion_code",
        zone_diffusion_type                             AS "zone_diffusion_type",
        zone_diffusion_libelle                          AS "zone_diffusion_nom",
        CONCAT(id_res, '_', zone_diffusion_unique_code) AS "id",
        CASE WHEN prescriptible
            THEN ARRAY['demandeur-demploi']::TEXT[]
        END                                             AS "profils",
        ARRAY(
            SELECT di_thematique_by_odspep_type_res_part.thematique
            FROM di_thematique_by_odspep_type_res_part
            WHERE ressources_partenariales.type_res_part = di_thematique_by_odspep_type_res_part.type_res_part
        )::TEXT[]                                       AS "thematiques"
    FROM ressources_partenariales
    ORDER BY 1

)

SELECT * FROM final
