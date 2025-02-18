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
        ARRAY(
            SELECT unnest_value
            FROM
                UNNEST(ARRAY[
                    CASE
                        WHEN
                            (services.modes_orientation_beneficiaire IS NOT NULL AND ARRAY_LENGTH(services.modes_orientation_beneficiaire, 1) > 0)
                            OR (services.modes_orientation_beneficiaire_autres IS NOT NULL AND services.modes_orientation_beneficiaire_autres != '') THEN 'usagers'
                    END,
                    CASE
                        WHEN
                            (services.modes_orientation_accompagnateur IS NOT NULL AND ARRAY_LENGTH(services.modes_orientation_accompagnateur, 1) > 0)
                            OR (services.modes_orientation_accompagnateur_autres IS NOT NULL AND services.modes_orientation_accompagnateur_autres != '') THEN 'professionnels'
                    END
                ]) AS unnest_value
            WHERE unnest_value IS NOT NULL
        )                                                AS "mobilisable_par",
        services.nom                                     AS "nom",
        services.presentation_detail                     AS "presentation_detail",
        services.presentation_resume                     AS "presentation_resume",
        services.prise_rdv                               AS "prise_rdv",
        services.profils                                 AS "profils",
        NULL                                             AS "profils_precisions",
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
        CAST(NULL AS DATE)                               AS "date_suspension",
        structures.id || '-' || services.id              AS "id"
    FROM services
    CROSS JOIN structures_with_commune AS structures
    -- Service ID 9 (Bilan/Accompagnement mobilité) is not available in Lyon anymore
    WHERE NOT (services.id = '9' AND structures.code_insee LIKE '69%')
)

SELECT * FROM final
