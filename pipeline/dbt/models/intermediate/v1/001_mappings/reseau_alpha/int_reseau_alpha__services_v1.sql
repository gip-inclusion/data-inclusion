WITH formations AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__formations') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__structures') }}
),

pages AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__pages') }}
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

final AS (
    SELECT
        'reseau-alpha'                                                                                    AS "source",
        'reseau-alpha--' || formations.id                                                                 AS "id",
        'reseau-alpha--' || 'formation-' || formations.id                                                 AS "adresse_id",
        'reseau-alpha--' || structures.id                                                                 AS "structure_id",
        formations.nom                                                                                    AS "nom",
        CASE WHEN LENGTH(formations.presentation_publique) >= 5 THEN formations.presentation_publique END AS "description",
        formations.url                                                                                    AS "lien_source",
        pages.date_derniere_modification                                                                  AS "date_maj",
        'formation'                                                                                       AS "type",
        ARRAY['lecture-ecriture-calcul--maitriser-le-francais']                                           AS "thematiques",
        CASE
            WHEN formations.cout = 'gratuit' THEN 'gratuit'
            WHEN formations.cout IS NOT NULL THEN 'payant'
        END                                                                                               AS "frais",
        formations.cout                                                                                   AS "frais_precisions",
        COALESCE(publics.publics, ARRAY['tous-publics'])                                                  AS "publics",
        NULL                                                                                              AS "publics_precisions",  -- TODO
        NULL                                                                                              AS "conditions_acces",  -- TODO
        COALESCE(formations.contact__telephone1, formations.contact__telephone2)                          AS "telephone",
        formations.contact__email                                                                         AS "courriel",
        formations.contact__nom || ' ' || formations.contact__prenom                                      AS "contact_nom_prenom",
        ARRAY['en-presentiel']                                                                            AS "modes_accueil",
        NULL                                                                                              AS "zone_eligibilite",
        CASE
            WHEN publics.habitant_un_territoire_precis THEN 'commune'
            ELSE 'departement'
        END                                                                                               AS "zone_eligibilite_type",
        NULL                                                                                              AS "lien_mobilisation",
        NULLIF(
            ARRAY_REMOVE(
                ARRAY[
                    CASE WHEN formations.contact__email IS NOT NULL THEN 'envoyer-un-courriel' END,
                    CASE WHEN COALESCE(formations.contact__telephone1, formations.contact__telephone2) IS NOT NULL THEN 'telephoner' END
                ],
                NULL
            ),
            '{}'
        )                                                                                                 AS "modes_mobilisation",
        CASE
            WHEN publics.oriente_par_des_prescripteurs
                THEN ARRAY['professionnels']
            ELSE ARRAY['professionnels', 'usagers']
        END                                                                                               AS "mobilisable_par",
        NULL                                                                                              AS "mobilisation_precisions",
        NULL                                                                                              AS "volume_horaire_hebdomadaire",
        (formations.date_fin - formations.date_debut) / 7                                                 AS "nombre_semaines",
        NULL                                                                                              AS "horaires_accueil"  -- TODO
    FROM formations
    LEFT JOIN structures ON formations.structure_id = structures.id
    LEFT JOIN pages ON structures.id = pages.structure_id
    LEFT JOIN publics ON formations.id = publics.formation_id
    WHERE
        formations.date_fin > CURRENT_DATE
)

SELECT * FROM final
