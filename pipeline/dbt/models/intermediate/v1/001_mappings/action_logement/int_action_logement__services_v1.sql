WITH services AS (
    SELECT * FROM {{ ref('stg_action_logement__services') }}
),

structures AS (
    SELECT * FROM {{ ref('int_action_logement__structures_v1') }}
),

final AS (
    SELECT
        'action-logement'                   AS "source",
        structures.id || '-' || services.id AS "id",
        structures.id                       AS "adresse_id",
        structures.id                       AS "structure_id",
        NULL                                AS "nom",
        NULL                                AS "description",
        NULL                                AS "lien_source",
        CAST(NULL AS DATE)                  AS "date_maj",
        NULL                                AS "type",
        CAST(NULL AS TEXT [])               AS "thematiques",
        NULL                                AS "frais",
        NULL                                AS "frais_precisions",
        CAST(NULL AS TEXT [])               AS "publics",
        NULL                                AS "publics_precisions",
        NULL                                AS "conditions_acces",
        NULL                                AS "telephone",
        NULL                                AS "courriel",
        CAST(NULL AS TEXT [])               AS "modes_accueil",
        CAST(NULL AS TEXT [])               AS "zone_eligibilite",
        'departement'                       AS "zone_eligibilite_type",
        NULL                                AS "contact_nom_prenom",
        NULL                                AS "lien_mobilisation",
        CAST(NULL AS TEXT [])               AS "modes_mobilisation",
        CAST(NULL AS TEXT [])               AS "mobilisable_par",
        NULL                                AS "mobilisation_precisions",
        CAST(NULL AS FLOAT)                 AS "volume_horaire_hebdomadaire",
        CAST(NULL AS INTEGER)               AS "nombre_semaines",
        NULL                                AS "horaires_accueil"
    FROM services
    CROSS JOIN structures
)

SELECT * FROM final
