WITH structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

services AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__services') }}
),

thematiques AS (
    SELECT
        services.id,
        ARRAY_AGG(DISTINCT map.thematique ORDER BY map.thematique) AS "thematiques"
    FROM services
    CROSS JOIN LATERAL UNNEST(services.services) AS s (service)
    INNER JOIN {{ ref('_map_mediation_numerique__thematiques') }} AS map
        ON s.service = map.service
    GROUP BY services.id
),

frais AS (
    SELECT
        services.id,
        ARRAY_AGG(DISTINCT map.frais ORDER BY map.frais) AS "frais"
    FROM services
    CROSS JOIN LATERAL UNNEST(services.frais_a_charge) AS f (frais_source)
    INNER JOIN {{ ref('_map_mediation_numerique__frais') }} AS map
        ON f.frais_source = map.frais_source
    GROUP BY services.id
),

modes_mobilisation AS (
    SELECT
        services.id,
        ARRAY_AGG(DISTINCT map.mode_mobilisation ORDER BY map.mode_mobilisation) AS "modes_mobilisation"
    FROM services
    CROSS JOIN LATERAL UNNEST(services.modalites_acces) AS m (modalite_source)
    INNER JOIN {{ ref('_map_mediation_numerique__modalites_acces') }} AS map
        ON m.modalite_source = map.modalite_source
    GROUP BY services.id
),

modes_accueil AS (
    SELECT
        services.id,
        ARRAY_AGG(DISTINCT map.mode_accueil ORDER BY map.mode_accueil) AS "modes_accueil"
    FROM services
    CROSS JOIN LATERAL UNNEST(services.modalites_accompagnement) AS m (modalite_source)
    INNER JOIN {{ ref('_map_mediation_numerique__modalites_accompagnement') }} AS map
        ON m.modalite_source = map.modalite_source
    GROUP BY services.id
),

publics_from_adresses AS (
    SELECT
        services.id,
        ARRAY_AGG(DISTINCT map.public ORDER BY map.public) AS "publics"
    FROM services
    CROSS JOIN LATERAL UNNEST(services.publics_specifiquement_adresses) AS p (public_source)
    INNER JOIN {{ ref('_map_mediation_numerique__publics') }} AS map
        ON p.public_source = map.public_source
    GROUP BY services.id
),

publics_from_prise_en_charge AS (
    SELECT
        services.id,
        ARRAY_AGG(DISTINCT map.public ORDER BY map.public) FILTER (WHERE map.public IS NOT NULL) AS "publics"
    FROM services
    CROSS JOIN LATERAL UNNEST(services.prise_en_charge_specifique) AS p (prise_en_charge_source)
    INNER JOIN {{ ref('_map_mediation_numerique__prise_en_charge') }} AS map
        ON p.prise_en_charge_source = map.prise_en_charge_source
    GROUP BY services.id
),

final AS (
    SELECT
        'mediation-numerique'                                                                          AS "source",
        'mediation-numerique--' || structures.id                                                       AS "structure_id",
        'mediation-numerique--' || structures.id                                                       AS "adresse_id",
        'mediation-numerique--' || services.id                                                         AS "id",
        structures.courriel                                                                            AS "courriel",
        structures.contact_nom_prenom                                                                  AS "contact_nom_prenom",
        CAST(services.date_maj AS DATE)                                                                AS "date_maj",
        'Service d''accompagnement au numérique'                                                      AS "nom",
        COALESCE(
            structures.presentation_detail, structures.presentation_resume, ARRAY_TO_STRING(services.services, ', ')
        )                                                                                              AS "description",
        -- very slow to load
        'https://cartographie.societenumerique.gouv.fr/cartographie/' || services.id || '/details'     AS "lien_source",
        'accompagnement'                                                                               AS "type",
        COALESCE(thematiques.thematiques, ARRAY['numerique--maitriser-les-fondamentaux-du-numerique']) AS "thematiques",
        COALESCE((frais.frais)[1], 'gratuit')                                                          AS "frais",
        NULL                                                                                           AS "frais_precisions",
        COALESCE(
            publics_from_adresses.publics || publics_from_prise_en_charge.publics,
            publics_from_adresses.publics,
            publics_from_prise_en_charge.publics,
            ARRAY['tous-publics']
        )                                                                                              AS "publics",
        NULLIF(
            ARRAY_TO_STRING(
                ARRAY_REMOVE(
                    services.publics_specifiquement_adresses || services.prise_en_charge_specifique,
                    NULL
                ),
                ', '
            ),
            ''
        )                                                                                              AS "publics_precisions",
        NULL                                                                                           AS "conditions_acces",
        structures.telephone                                                                           AS "telephone",
        COALESCE(
            modes_mobilisation.modes_mobilisation,
            NULLIF(
                ARRAY_REMOVE(
                    ARRAY[
                        CASE WHEN structures.telephone IS NOT NULL THEN 'telephoner' END,
                        CASE WHEN structures.courriel IS NOT NULL THEN 'envoyer-un-courriel' END
                    ],
                    NULL
                ),
                '{}'
            )
        )                                                                                              AS "modes_mobilisation",
        services.prise_rdv                                                                             AS "lien_mobilisation",
        ARRAY['usagers', 'professionnels']                                                             AS "mobilisable_par",
        NULL                                                                                           AS "mobilisation_precisions",
        COALESCE(modes_accueil.modes_accueil, ARRAY['en-presentiel'])                                  AS "modes_accueil",
        CASE
            WHEN structures.adresse__code_insee LIKE '97%' THEN ARRAY[LEFT(structures.adresse__code_insee, 3)]
            WHEN structures.adresse__code_insee IS NOT NULL THEN ARRAY[LEFT(structures.adresse__code_insee, 2)]
        END                                                                                            AS "zone_eligibilite",
        'departement'                                                                                  AS "zone_eligibilite_type",
        NULL                                                                                           AS "volume_horaire_hebdomadaire",
        NULL                                                                                           AS "nombre_semaines",
        structures.horaires                                                                            AS "horaires_accueil"
    FROM services
    LEFT JOIN structures ON services.structure_id = structures.id
    LEFT JOIN thematiques ON services.id = thematiques.id
    LEFT JOIN frais ON services.id = frais.id
    LEFT JOIN modes_mobilisation ON services.id = modes_mobilisation.id
    LEFT JOIN modes_accueil ON services.id = modes_accueil.id
    LEFT JOIN publics_from_adresses ON services.id = publics_from_adresses.id
    LEFT JOIN publics_from_prise_en_charge ON services.id = publics_from_prise_en_charge.id
)

SELECT * FROM final
