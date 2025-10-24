WITH services AS (
    SELECT * FROM {{ ref('stg_action_logement__services') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_action_logement__structures') }}
),

communes AS (
    SELECT DISTINCT ON (code_postal)
        code_postal,
        code_departement
    FROM {{ ref('stg_decoupage_administratif__communes') }},
        LATERAL UNNEST(codes_postaux) AS code_postal
),

final AS (
    SELECT
        'action-logement'                                          AS "source",
        'action-logement--' || structures.id                       AS "structure_id",
        'action-logement--' || structures.id                       AS "adresse_id",
        'action-logement--' || structures.id || '-' || services.id AS "id",
        services.courriel                                          AS "courriel",
        services.contact_nom_prenom                                AS "contact_nom_prenom",
        services.date_maj                                          AS "date_maj",
        services.nom                                               AS "nom",
        services.description                                       AS "description",
        services.lien_source                                       AS "lien_source",
        services.type                                              AS "type",
        services.thematiques                                       AS "thematiques",
        services.frais                                             AS "frais",
        services.frais_precisions                                  AS "frais_precisions",
        services.publics                                           AS "publics",
        services.publics_precisions                                AS "publics_precisions",
        services.conditions_acces                                  AS "conditions_acces",
        services.telephone                                         AS "telephone",
        services.modes_mobilisation                                AS "modes_mobilisation",
        services.lien_mobilisation                                 AS "lien_mobilisation",
        services.mobilisable_par                                   AS "mobilisable_par",
        services.mobilisation_precisions                           AS "mobilisation_precisions",
        services.modes_accueil                                     AS "modes_accueil",
        CASE
            WHEN services.zone_eligibilite IS NOT NULL
                THEN services.zone_eligibilite
            WHEN communes.code_departement IS NOT NULL
                THEN ARRAY[communes.code_departement]
        END                                                        AS "zone_eligibilite",
        services.volume_horaire_hebdomadaire                       AS "volume_horaire_hebdomadaire",
        services.nombre_semaines                                   AS "nombre_semaines",
        structures.horaires_accueil                                AS "horaires_accueil"
    FROM services
    CROSS JOIN structures
    LEFT JOIN communes
        ON structures.code_postal = communes.code_postal
)

SELECT * FROM final
