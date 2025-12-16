WITH structures AS (
    SELECT * FROM {{ ref('stg_ma_boussole_aidants__structures') }}
),

services AS (
    SELECT * FROM {{ ref('stg_ma_boussole_aidants__structures__services') }}
),

solutions AS (
    SELECT * FROM {{ ref('stg_ma_boussole_aidants__solutions') }}
),

situations AS (
    SELECT * FROM {{ ref('stg_ma_boussole_aidants__structures__situations') }}
),

deduped_services AS (
    SELECT
        id_structure,
        id_sous_thematique_solutions,
        ARRAY_AGG(
            CASE
                WHEN is_distanciel THEN 'a-distance'
                WHEN NOT is_distanciel THEN 'en-presentiel'
            END
        ) AS "modes_accueil"
    FROM services
    GROUP BY
        id_structure,
        id_sous_thematique_solutions
),

publics AS (
    SELECT
        situations.id_structure,
        ARRAY_AGG(
            DISTINCT mapping.public
            ORDER BY mapping.public
        ) AS publics
    FROM situations
    INNER JOIN {{ ref('_map_ma_boussole_aidants__publics') }} AS "mapping"
        ON situations.id_situation = mapping.situation_id
    GROUP BY situations.id_structure
),

thematiques AS (
    SELECT
        services.id_sous_thematique_solutions,
        ARRAY_AGG(
            DISTINCT mapping.thematique
            ORDER BY mapping.thematique
        ) AS thematiques
    FROM deduped_services AS services
    INNER JOIN {{ ref('_map_ma_boussole_aidants__thematiques') }} AS "mapping"
        ON services.id_sous_thematique_solutions = mapping.solution_id
    GROUP BY services.id_sous_thematique_solutions
),

final AS (
    SELECT
        'ma-boussole-aidants'                                                                               AS "source",
        'ma-boussole-aidants--' || structures.id_structure                                                  AS "structure_id",
        'ma-boussole-aidants--' || structures.id_structure                                                  AS "adresse_id",
        'ma-boussole-aidants--' || structures.id_structure || '--' || services.id_sous_thematique_solutions AS "id",
        structures.email                                                                                    AS "courriel",
        NULL                                                                                                AS "contact_nom_prenom",
        structures.last_modified_date                                                                       AS "date_maj",
        solutions.label                                                                                     AS "nom",
        solutions.description                                                                               AS "description",
        CASE
            WHEN structures.departement__code_departement IS NOT NULL
                THEN
                    FORMAT(
                        'https://maboussoleaidants.fr/mes-solutions/%s/%s/%s',
                        CASE
                            WHEN structures.id_type_structure = '15' THEN 'centre-communal-action-sociale-ccas'
                            WHEN structures.id_type_structure = '53' THEN 'maison-departementale-autonomie-mda'
                            WHEN structures.id_type_structure = '127' THEN 'maison-departementale-des-solidarites'
                        END,
                        structures.departement__code_departement,
                        structures.id_structure
                    )
        END                                                                                                 AS "lien_source",
        types.type                                                                                          AS "type",
        thematiques.thematiques                                                                             AS "thematiques",
        'gratuit'                                                                                           AS "frais",
        NULL                                                                                                AS "frais_precisions",
        publics.publics                                                                                     AS "publics",
        ARRAY_TO_STRING(
            ARRAY[
                'Âge minimum : ' || structures.age_min,
                'Âge maximum : ' || structures.age_max
            ],
            ' '
        )                                                                                                   AS "publics_precisions",
        NULL                                                                                                AS "conditions_acces",
        structures.telephone_1                                                                              AS "telephone",
        NULLIF(
            ARRAY_REMOVE(
                ARRAY[
                    CASE WHEN structures.telephone_1 IS NOT NULL THEN 'telephoner' END,
                    CASE WHEN structures.email IS NOT NULL THEN 'envoyer-un-courriel' END
                ],
                NULL
            ),
            '{}'
        )                                                                                                   AS "modes_mobilisation",
        NULL                                                                                                AS "lien_mobilisation",
        ARRAY['usagers', 'professionnels']                                                                  AS "mobilisable_par",
        NULL                                                                                                AS "mobilisation_precisions",
        services.modes_accueil                                                                              AS "modes_accueil",
        CAST(NULL AS TEXT [])                                                                               AS "zone_eligibilite",
        CASE structures.rayon_action__nom_rayon_action
            WHEN 'communal' THEN 'commune'
            WHEN 'départemental' THEN 'departement'
            WHEN 'local' THEN 'region'
            ELSE 'pays'
        END                                                                                                 AS "zone_eligibilite_type",
        NULL                                                                                                AS "volume_horaire_hebdomadaire",
        NULL                                                                                                AS "nombre_semaines",
        NULL                                                                                                AS "horaires_accueil"
    FROM deduped_services AS services
    LEFT JOIN structures ON services.id_structure = structures.id_structure
    LEFT JOIN solutions ON services.id_sous_thematique_solutions = solutions.code
    LEFT JOIN thematiques ON services.id_sous_thematique_solutions = thematiques.id_sous_thematique_solutions
    LEFT JOIN {{ ref('_map_ma_boussole_aidants__types') }} AS "types" ON services.id_sous_thematique_solutions = types.solution_id
    LEFT JOIN publics ON services.id_structure = publics.id_structure
)

SELECT * FROM final
