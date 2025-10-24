WITH services AS (
    SELECT * FROM {{ ref('stg_action_logement__services_v1') }}
),

structures AS (
    SELECT * FROM {{ ref('int_action_logement__structures_v1') }}
),

final AS (
    SELECT
        'action-logement'                                         AS "source",
        'action-logement--' || structures.id                     AS "structure_id",
        'action-logement--' || structures.id                     AS "adresse_id",
        'action-logement--' || structures.id || '-' || services.id AS "id",
        NULL AS "courriel",
        NULL                                                      AS "contact_nom_prenom",
        services.date_maj                                       AS "date_maj",
        services.nom                                           AS "nom",
        services.description                                   AS "description",
        services.lien_source                                     AS "lien_source",
        services.type AS "type",
        services.thematiques AS "thematiques",
        services.frais AS "frais",
        NULL AS "frais_autres",
        services.publics AS "publics",
        services.publics_precisions AS "publics_precisions",
        services.conditions_acces AS "conditions_acces",
        services.modes_mobilisation AS "modes_mobilisation",
        NULL AS "mobilisation_precisions",
        services.modes_accueil AS "modes_accueil",
        -- NOTE(vperron): take the departement from which the structure comes from
        NULL AS "zone_eligibilite",
        NULL AS "volume_horaire_hebdomadaire",
        NULL AS "nombre_semaines",
        structures.horaires_accueil AS "horaires_accueil"
    FROM services
    CROSS JOIN structures
)

SELECT * FROM final
