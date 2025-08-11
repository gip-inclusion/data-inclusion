WITH services AS (
    SELECT * FROM {{ ref('stg_france_travail__services') }}
),

agences AS (
    SELECT * FROM {{ ref('int_france_travail__structures') }}
),

adresses AS (
    SELECT * FROM {{ ref('int_france_travail__adresses') }}
),

structures_with_commune AS (
    SELECT
        agences.*,
        adresses.code_insee AS "code_insee",
        adresses.commune    AS "commune"
    FROM agences
    LEFT JOIN adresses ON agences.id = adresses.id
),

final AS (
    SELECT
        'france-travail'                                                                 AS "source",
        structures.id                                                                    AS "structure_id",
        structures.id                                                                    AS "adresse_id",
        NULLIF(ARRAY_TO_STRING(services.pre_requis || services.justificatifs, '\n'), '') AS "conditions_acces",
        structures.courriel                                                              AS "courriel",
        NULL                                                                             AS "contact_nom_prenom",
        structures.date_maj                                                              AS "date_maj",
        services.formulaire_en_ligne                                                     AS "formulaire_en_ligne",
        services.frais_autres                                                            AS "frais_autres",
        services.justificatifs                                                           AS "justificatifs",
        NULL                                                                             AS "lien_source",
        services.modes_accueil                                                           AS "modes_accueil",
        services.modes_orientation_accompagnateur                                        AS "modes_orientation_accompagnateur",
        services.modes_orientation_accompagnateur_autres                                 AS "modes_orientation_accompagnateur_autres",
        services.modes_orientation_beneficiaire                                          AS "modes_orientation_beneficiaire",
        services.modes_orientation_beneficiaire_autres                                   AS "modes_orientation_beneficiaire_autres",
        CAST(NULL AS TEXT [])                                                            AS "modes_mobilisation",
        CAST(NULL AS TEXT [])                                                            AS "mobilisable_par",
        NULL                                                                             AS "mobilisation_precisions",
        services.nom                                                                     AS "nom",
        services.presentation_detail                                                     AS "presentation_detail",
        CASE
            WHEN LENGTH(services.presentation_resume) <= 280
                THEN services.presentation_resume
            ELSE LEFT(services.presentation_resume, 279) || '…'
        END                                                                              AS "presentation_resume",
        services.prise_rdv                                                               AS "prise_rdv",
        NULL                                                                             AS "lien_mobilisation",
        services.profils                                                                 AS "profils",
        NULL                                                                             AS "profils_precisions",
        services.pre_requis                                                              AS "pre_requis",
        services.recurrence                                                              AS "recurrence",
        services.thematiques                                                             AS "thematiques",
        services.types                                                                   AS "types",
        structures.telephone                                                             AS "telephone",
        services.frais                                                                   AS "frais",
        'commune'                                                                        AS "zone_diffusion_type",
        structures.code_insee                                                            AS "zone_diffusion_code",
        structures.commune                                                               AS "zone_diffusion_nom",
        NULL                                                                             AS "page_web",
        CAST(NULL AS FLOAT)                                                              AS "volume_horaire_hebdomadaire",
        CAST(NULL AS INT)                                                                AS "nombre_semaines",
        structures.id || '-' || services.id                                              AS "id"
    FROM services
    CROSS JOIN structures_with_commune AS structures
    -- Service ID 9 (Bilan/Accompagnement mobilité) is not available in Lyon anymore
    WHERE NOT (services.id = '9' AND structures.code_insee LIKE '69%')
)

SELECT * FROM final
