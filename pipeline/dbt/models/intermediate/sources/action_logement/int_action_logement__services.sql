WITH services AS (
    SELECT * FROM {{ ref('stg_action_logement__services') }}
),

structures AS (
    SELECT * FROM {{ ref('int_action_logement__structures') }}
),

final AS (
    SELECT
        services._di_source_id                           AS "source",
        structures.id                                    AS "structure_id",
        structures.id                                    AS "adresse_id",
        structures.courriel                              AS "courriel",
        services.cumulable                               AS "cumulable",
        TRUE                                             AS "contact_public",
        NULL                                             AS "contact_nom_prenom",
        structures.date_maj                              AS "date_maj",
        structures.date_maj                              AS "date_creation",
        services.formulaire_en_ligne                     AS "formulaire_en_ligne",
        services.frais_autres                            AS "frais_autres",
        services.justificatifs                           AS "justificatifs",
        NULL                                             AS "lien_source",
        services.modes_accueil                           AS "modes_accueil",
        services.modes_orientation_accompagnateur        AS "modes_orientation_accompagnateur",
        services.modes_orientation_accompagnateur_autres AS "modes_orientation_accompagnateur_autres",
        services.modes_orientation_beneficiaire          AS "modes_orientation_beneficiaire",
        services.modes_orientation_beneficiaire_autres   AS "modes_orientation_beneficiaire_autres",
        services.nom                                     AS "nom",
        services.page_web                                AS "page_web",
        services.presentation_detail                     AS "presentation_detail",
        services.presentation_resume                     AS "presentation_resume",
        services.prise_rdv                               AS "prise_rdv",
        services.profils                                 AS "profils",
        services.pre_requis                              AS "pre_requis",
        services.recurrence                              AS "recurrence",
        services.thematiques                             AS "thematiques",
        services.types                                   AS "types",
        structures.telephone                             AS "telephone",
        services.frais                                   AS "frais",
        'departement'                                    AS "zone_diffusion_type",
        NULL                                             AS "zone_diffusion_code",
        NULL                                             AS "zone_diffusion_nom",
        CAST(NULL AS DATE)                               AS "date_suspension",
        structures.id || '-' || services.id              AS "id"
    FROM services
    CROSS JOIN structures
)

SELECT * FROM final
