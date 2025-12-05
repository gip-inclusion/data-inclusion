WITH organisations AS (
    SELECT * FROM {{ ref('stg_cd35__organisations') }}
),

organisations_thematiques AS (
    SELECT * FROM {{ ref('stg_cd35__organisations__thematiques') }}
),

organisations_profils AS (
    SELECT * FROM {{ ref('stg_cd35__organisations__profils') }}
),

mapping_types AS (
    SELECT * FROM {{ ref('_map_cd35__types') }}
),

mapping_thematiques AS (
    SELECT * FROM {{ ref('_map_cd35__thematiques') }}
),

publics AS (
    SELECT
        organisations_profils.structure_id,
        ARRAY_AGG(DISTINCT mapping.public) AS publics
    FROM organisations_profils
    INNER JOIN {{ ref('_map_cd35__publics') }} AS "mapping"
        ON organisations_profils.value = mapping.profil
    GROUP BY organisations_profils.structure_id
),

final AS (
    SELECT DISTINCT ON (4)
        'cd35'                                                                  AS "source",
        'cd35--' || organisations.id                                            AS "structure_id",
        'cd35--' || organisations.id                                            AS "adresse_id",
        'cd35--' || organisations.id || '--' || organisations_thematiques.value AS "id",
        organisations.courriel                                                  AS "courriel",
        NULL                                                                    AS "contact_nom_prenom",
        organisations.date_maj                                                  AS "date_maj",
        organisations_thematiques.value                                         AS "nom",
        organisations.presentation_detail                                       AS "description",
        organisations.lien_source                                               AS "lien_source",
        mapping_types.type                                                      AS "type",
        ARRAY[mapping_thematiques.data_inclusion]                               AS "thematiques",
        NULL                                                                    AS "frais",
        NULL                                                                    AS "frais_precisions",
        publics.publics                                                         AS "publics",
        NULL                                                                    AS "publics_precisions",
        NULL                                                                    AS "conditions_acces",
        organisations.telephone                                                 AS "telephone",
        NULLIF(
            ARRAY_REMOVE(
                ARRAY[
                    CASE
                        WHEN organisations.telephone IS NOT NULL
                            THEN 'telephoner'
                    END,
                    CASE
                        WHEN organisations.courriel IS NOT NULL
                            THEN 'envoyer-un-courriel'
                    END
                ],
                NULL
            ),
            '{}'
        )                                                                       AS "modes_mobilisation",
        NULL                                                                    AS "lien_mobilisation",
        ARRAY['usagers', 'professionnels']                                      AS "mobilisable_par",
        NULL                                                                    AS "mobilisation_precisions",
        NULL                                                                    AS "modes_accueil",
        ARRAY['35']                                                             AS "zone_eligibilite",
        NULL                                                                    AS "volume_horaire_hebdomadaire",
        NULL                                                                    AS "nombre_semaines",
        organisations.horaires_ouvertures                                       AS "horaires_accueil"
    FROM organisations_thematiques
    INNER JOIN organisations
        ON organisations_thematiques.structure_id = organisations.id
    LEFT JOIN publics
        ON organisations_thematiques.structure_id = publics.structure_id
    LEFT JOIN mapping_types
        ON organisations_thematiques.value = mapping_types.thematique
    LEFT JOIN mapping_thematiques
        ON organisations_thematiques.value = mapping_thematiques.cd35
    -- ignore unmapped thematiques
    WHERE mapping_thematiques.data_inclusion IS NOT NULL
)

SELECT * FROM final
