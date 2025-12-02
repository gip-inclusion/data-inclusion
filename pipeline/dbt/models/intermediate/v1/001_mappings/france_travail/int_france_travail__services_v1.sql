WITH services AS (
    SELECT * FROM {{ ref('stg_france_travail__services') }}
),

agences AS (
    SELECT * FROM {{ ref('stg_france_travail__agences') }}
),

communes AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__communes') }}
),

final AS (
    SELECT
        'france-travail'                                                                                AS "source",
        'france-travail--' || agences.code                                                              AS "structure_id",
        'france-travail--' || agences.code                                                              AS "adresse_id",
        'france-travail--' || agences.code || '-' || services.id                                        AS "id",
        -- FT does not want us to use email addresses of their agencies
        services.courriel                                                                               AS "courriel",
        services.contact_nom_prenom                                                                     AS "contact_nom_prenom",
        services.date_maj                                                                               AS "date_maj",
        services.nom                                                                                    AS "nom",
        services.description                                                                            AS "description",
        services.lien_source                                                                            AS "lien_source",
        services.type                                                                                   AS "type",
        services.thematiques                                                                            AS "thematiques",
        services.frais                                                                                  AS "frais",
        services.frais_precisions                                                                       AS "frais_precisions",
        services.publics                                                                                AS "publics",
        services.publics_precisions                                                                     AS "publics_precisions",
        services.conditions_acces                                                                       AS "conditions_acces",
        services.telephone                                                                              AS "telephone",
        services.modes_mobilisation                                                                     AS "modes_mobilisation",
        services.lien_mobilisation                                                                      AS "lien_mobilisation",
        services.mobilisable_par                                                                        AS "mobilisable_par",
        services.mobilisation_precisions                                                                AS "mobilisation_precisions",
        services.modes_accueil                                                                          AS "modes_accueil",
        CASE
            WHEN services.zone_eligibilite IS NOT NULL
                THEN services.zone_eligibilite
            WHEN communes.code_departement IS NOT NULL
                THEN ARRAY[communes.code_departement]
        END                                                                                             AS "zone_eligibilite",
        services.volume_horaire_hebdomadaire                                                            AS "volume_horaire_hebdomadaire",
        services.nombre_semaines                                                                        AS "nombre_semaines",
        COALESCE(services.horaires_accueil, processings.france_travail_opening_hours(agences.horaires)) AS "horaires_accueil"
    FROM services
    CROSS JOIN agences
    LEFT JOIN communes ON agences.adresse_principale__commune_implantation = communes.code
)

SELECT * FROM final
