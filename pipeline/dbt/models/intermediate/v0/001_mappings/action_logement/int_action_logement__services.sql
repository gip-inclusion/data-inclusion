WITH services AS (
    SELECT * FROM {{ ref('stg_action_logement__services') }}
),

structures AS (
    SELECT * FROM {{ ref('int_action_logement__structures') }}
),

final AS (
    SELECT
        'action-logement'                                         AS "source",
        structures.id                                             AS "structure_id",
        structures.id                                             AS "adresse_id",
        structures.courriel                                       AS "courriel",
        NULL                                                      AS "contact_nom_prenom",
        structures.date_maj                                       AS "date_maj",
        services.formulaire_en_ligne                              AS "formulaire_en_ligne",
        services.frais_autres                                     AS "frais_autres",
        services.justificatifs                                    AS "justificatifs",
        services.lien_source                                      AS "lien_source",
        services.modes_accueil                                    AS "modes_accueil",
        CAST(NULL AS TEXT [])                                     AS "modes_mobilisation",
        services.modes_orientation_accompagnateur                 AS "modes_orientation_accompagnateur",
        services.modes_orientation_accompagnateur_autres          AS "modes_orientation_accompagnateur_autres",
        services.modes_orientation_beneficiaire                   AS "modes_orientation_beneficiaire",
        services.modes_orientation_beneficiaire_autres            AS "modes_orientation_beneficiaire_autres",
        services.nom                                              AS "nom",
        services.page_web                                         AS "page_web",
        services.presentation_detail                              AS "presentation_detail",
        services.presentation_resume                              AS "presentation_resume",
        services.prise_rdv                                        AS "prise_rdv",
        services.profils                                          AS "profils",
        NULL                                                      AS "profils_precisions",
        services.pre_requis                                       AS "pre_requis",
        services.recurrence                                       AS "recurrence",
        services.thematiques                                      AS "thematiques",
        services.types                                            AS "types",
        structures.telephone                                      AS "telephone",
        services.frais                                            AS "frais",
        CAST(NULL AS FLOAT)                                       AS "volume_horaire_hebdomadaire",
        CAST(NULL AS INT)                                         AS "nombre_semaines",
        'departement'                                             AS "zone_diffusion_type",
        NULL                                                      AS "zone_diffusion_code",
        NULL                                                      AS "zone_diffusion_nom",
        structures.id || '-' || services.id                       AS "id"
    FROM services
    CROSS JOIN structures
)

SELECT * FROM final
