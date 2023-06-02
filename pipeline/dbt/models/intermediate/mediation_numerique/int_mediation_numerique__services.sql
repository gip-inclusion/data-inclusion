WITH services AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__services') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

final AS (
    SELECT
        services.id                                                                                    AS "id",
        services.nom                                                                                   AS "nom",
        NULL                                                                                           AS "presentation_resume",
        NULL                                                                                           AS "presentation_detail",
        services.prise_rdv                                                                             AS "prise_rdv",
        services.frais                                                                                 AS "frais",
        NULL::TEXT                                                                                     AS "frais_autres",
        services.profils                                                                               AS "profils",
        services.structure_id                                                                          AS "structure_id",
        services.thematiques                                                                           AS "thematiques",
        services._di_source_id                                                                         AS "source",
        NULL                                                                                           AS "pre_requis",
        NULL                                                                                           AS "cumulable",
        NULL                                                                                           AS "justificatifs",
        NULL                                                                                           AS "formulaire_en_ligne",
        structures.commune                                                                             AS "commune",
        structures.code_postal                                                                         AS "code_postal",
        NULL                                                                                           AS "code_insee",
        structures.adresse                                                                             AS "adresse",
        NULL                                                                                           AS "complement_adresse",
        structures.longitude                                                                           AS "longitude",
        structures.latitude                                                                            AS "latitude",
        NULL                                                                                           AS "recurrence",
        NULL                                                                                           AS "date_creation",
        NULL                                                                                           AS "date_suspension",
        NULL                                                                                           AS "lien_source",
        structures.telephone                                                                           AS "telephone",
        structures.courriel                                                                            AS "courriel",
        TRUE                                                                                           AS "contact_public",
        structures.date_maj                                                                            AS "date_maj",
        NULL                                                                                           AS "zone_diffusion_type",
        NULL                                                                                           AS "zone_diffusion_code",
        NULL                                                                                           AS "zone_diffusion_nom",
        CASE WHEN CARDINALITY(services.types) > 0 THEN services.types ELSE ARRAY['accompagnement'] END AS "types",
        ARRAY['en-presentiel']                                                                         AS "modes_accueil"
    FROM services
    LEFT JOIN structures ON services.structure_id = structures.id
)

SELECT * FROM final
