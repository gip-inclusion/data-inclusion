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

-- Some services do not exist anymore, therefore we were asked to remove them
excluded_ids AS (
    SELECT structures.id || '-' || services.id AS id
    FROM services
    CROSS JOIN structures_with_commune AS structures
    WHERE
        services.nom = 'Bilan/Accompagnement mobilité'
        AND structures.code_insee LIKE '69%'
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
        'commune'                                        AS "zone_diffusion_type",
        structures.code_insee                            AS "zone_diffusion_code",
        structures.commune                               AS "zone_diffusion_nom",
        NULL                                             AS "page_web",
        NULL::DATE                                       AS "date_suspension",
        structures.id || '-' || services.id              AS "id"
    FROM services
    CROSS JOIN structures_with_commune AS structures
    WHERE
        structures.id || '-' || services.id NOT IN (SELECT excluded_ids.id FROM excluded_ids)
)

SELECT * FROM final
