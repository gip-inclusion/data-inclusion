WITH formations AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__formations') }}
),

formations_adresses AS (
    SELECT DISTINCT ON (formation_id) *
    FROM {{ ref('stg_reseau_alpha__formations__adresses') }}
    ORDER BY formation_id, voie IS NOT NULL DESC
),

structures AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__structures') }}
),

pages AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__pages') }}
),

criteres_scolarisation AS (
    SELECT
        formation_id,
        STRING_AGG(DISTINCT '- ' || value, E'\n') AS "description"
    FROM {{ ref('stg_reseau_alpha__formations__criteres_scolarisation') }}
    GROUP BY formation_id
),

objectifs_vises AS (
    SELECT
        formation_id,
        STRING_AGG(FORMAT('%s %s', label, '[' || description || ']'), '. ') AS "description"
    FROM {{ ref('stg_reseau_alpha__formations__objectifs') }}
    GROUP BY formation_id
),

competences_visees AS (
    SELECT
        formation_id,
        STRING_AGG(DISTINCT value, ', ') AS "description"
    FROM {{ ref('stg_reseau_alpha__formations__competences_linguistiques') }}
    GROUP BY formation_id
),

formations__publics AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__formations__publics') }}
),

publics AS (
    SELECT
        formations__publics.formation_id,
        ARRAY_AGG(DISTINCT mapping.public_datainclusion)                                                       AS "publics",
        'habitant-un-territoire-precis' = ANY(ARRAY_AGG(DISTINCT formations__publics.public_specifique_value)) AS "habitant_un_territoire_precis",
        'oriente-par-des-prescripteurs' = ANY(ARRAY_AGG(DISTINCT formations__publics.public_specifique_value)) AS "oriente_par_des_prescripteurs"
    FROM formations__publics
    INNER JOIN {{ ref('_map_reseau_alpha__publics_v1') }} AS "mapping"
        ON formations__publics.public_specifique_value = mapping.public_reseau_alpha
    WHERE formations__publics.public_specifique_prioritaire = 'exclusif'
    GROUP BY formations__publics.formation_id
),

publics_precisions AS (
    SELECT
        formation_id,
        NULLIF(
            ARRAY_TO_STRING(
                ARRAY[
                    E'Publics exclusifs : \n\n' || (
                        STRING_AGG(
                            DISTINCT
                            CASE
                                WHEN public_specifique_value = 'habitant-un-territoire-precis'
                                    THEN '- ' || public_specifique_raw_value || ' : ' || public_specifique_description
                                ELSE '- ' || public_specifique_raw_value
                            END,
                            E'\n'
                        ) FILTER (WHERE public_specifique_prioritaire = 'exclusif')
                    ),
                    E'Publics prioritaires : \n\n' || (
                        STRING_AGG(
                            DISTINCT
                            CASE
                                WHEN public_specifique_value = 'habitant-un-territoire-precis'
                                    THEN '- ' || public_specifique_raw_value || ' : ' || public_specifique_description
                                ELSE '- ' || public_specifique_raw_value
                            END,
                            E'\n'
                        ) FILTER (WHERE public_specifique_prioritaire = 'prioritaire')
                    )
                ],
                E'\n\n'
            ),
            ''
        ) AS "description"
    FROM formations__publics
    GROUP BY formation_id
),

final AS (
    SELECT
        'reseau-alpha'                                                            AS "source",
        'reseau-alpha--' || formations.id                                         AS "id",
        'reseau-alpha--' || 'formation-' || formations_adresses.formation_id      AS "adresse_id",
        'reseau-alpha--' || structures.id                                         AS "structure_id",
        formations.nom                                                            AS "nom",
        ARRAY_TO_STRING(
            ARRAY[
                formations.presentation_publique,
                'Objectifs visés : ' || objectifs_vises.description,
                'Niveau de langue et de compétences visé par la formation : ' || competences_visees.description,
                CASE WHEN formations.place_disponible THEN 'Places disponibles' ELSE 'Pas de places disponibles' END,
                'Étendue de la formation : ' || TO_CHAR(formations.date_debut, 'DD TMmonth YYYY') || ' - ' || TO_CHAR(formations.date_fin, 'DD TMmonth YYYY')
            ],
            E'\n\n'
        )                                                                         AS "description",
        formations.url                                                            AS "lien_source",
        pages.date_derniere_modification                                          AS "date_maj",
        'formation'                                                               AS "type",
        ARRAY['lecture-ecriture-calcul--maitriser-le-francais']                   AS "thematiques",
        CASE
            WHEN formations.cout = 'gratuit' THEN 'gratuit'
            WHEN formations.cout IS NOT NULL THEN 'payant'
        END                                                                       AS "frais",
        formations.cout                                                           AS "frais_precisions",
        COALESCE(publics.publics, ARRAY['tous-publics'])                          AS "publics",
        publics_precisions.description                                            AS "publics_precisions",
        criteres_scolarisation.description                                        AS "conditions_acces",
        COALESCE(formations.contact__telephone1, formations.contact__telephone2)  AS "telephone",
        formations.contact__email                                                 AS "courriel",
        formations.contact__nom || ' ' || formations.contact__prenom              AS "contact_nom_prenom",
        ARRAY['en-presentiel']                                                    AS "modes_accueil",
        NULL                                                                      AS "zone_eligibilite",
        CASE
            WHEN publics.habitant_un_territoire_precis THEN 'commune'
            ELSE 'departement'
        END                                                                       AS "zone_eligibilite_type",
        NULL                                                                      AS "lien_mobilisation",
        NULLIF(
            ARRAY_REMOVE(
                ARRAY[
                    CASE WHEN formations.contact__email IS NOT NULL THEN 'envoyer-un-courriel' END,
                    CASE WHEN COALESCE(formations.contact__telephone1, formations.contact__telephone2) IS NOT NULL THEN 'telephoner' END
                ],
                NULL
            ),
            '{}'
        )                                                                         AS "modes_mobilisation",
        CASE
            WHEN publics.oriente_par_des_prescripteurs
                THEN ARRAY['professionnels']
            ELSE ARRAY['professionnels', 'usagers']
        END                                                                       AS "mobilisable_par",
        NULL                                                                      AS "mobilisation_precisions",
        NULL                                                                      AS "volume_horaire_hebdomadaire",
        NULLIF((formations.date_fin - formations.date_debut) / 7, 0)              AS "nombre_semaines",
        processings.reseau_alpha_opening_hours(formations.jours_horaires_details) AS "horaires_accueil"
    FROM formations
    LEFT JOIN structures ON formations.structure_id = structures.id
    LEFT JOIN pages ON structures.id = pages.structure_id
    LEFT JOIN publics ON formations.id = publics.formation_id
    LEFT JOIN competences_visees ON formations.id = competences_visees.formation_id
    LEFT JOIN objectifs_vises ON formations.id = objectifs_vises.formation_id
    LEFT JOIN criteres_scolarisation ON formations.id = criteres_scolarisation.formation_id
    LEFT JOIN publics_precisions ON formations.id = publics_precisions.formation_id
    LEFT JOIN formations_adresses ON formations.id = formations_adresses.formation_id
    WHERE
        formations.date_fin > CURRENT_DATE
)

SELECT * FROM final
