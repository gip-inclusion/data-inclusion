WITH structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

thematiques AS (
    SELECT
        services.structure_id,
        ARRAY_AGG(DISTINCT map.thematique ORDER BY map.thematique) AS "thematiques"
    FROM {{ ref('stg_mediation_numerique__structures__services') }} AS services
    INNER JOIN {{ ref('_map_mediation_numerique__thematiques') }} AS map
        ON services.value = map.service
    GROUP BY services.structure_id
),

services_raw AS (
    SELECT
        structure_id,
        ARRAY_AGG(value ORDER BY value) AS "services"
    FROM {{ ref('stg_mediation_numerique__structures__services') }}
    GROUP BY structure_id
),

frais AS (
    SELECT DISTINCT ON (structure_id)
        structure_id,
        CASE
            WHEN value = 'Gratuit' THEN 'gratuit'
            WHEN value = 'Gratuit sous condition' THEN 'gratuit'
            WHEN value = 'Payant' THEN 'payant'
        END AS "frais"
    FROM {{ ref('stg_mediation_numerique__structures__frais') }}
    WHERE value IS NOT NULL
    ORDER BY structure_id, value = 'Gratuit' DESC
),

modes_mobilisation AS (
    SELECT
        modalites_acces.structure_id,
        ARRAY_AGG(DISTINCT map.mode_mobilisation ORDER BY map.mode_mobilisation) AS "modes_mobilisation"
    FROM {{ ref('stg_mediation_numerique__structures__modalites_acces') }} AS modalites_acces
    INNER JOIN {{ ref('_map_mediation_numerique__modes_mobilisation') }} AS map
        ON modalites_acces.value = map.modalite
    GROUP BY modalites_acces.structure_id
),

modalites_accompagnement AS (
    SELECT
        structure_id,
        ARRAY_AGG(value ORDER BY value) AS "modalites"
    FROM {{ ref('stg_mediation_numerique__structures__modalites_accompagnement') }}
    GROUP BY structure_id
),

types AS (
    SELECT
        modalites_accompagnement.structure_id,
        ARRAY_AGG(DISTINCT map.type ORDER BY map.type) AS "types"
    FROM {{ ref('stg_mediation_numerique__structures__modalites_accompagnement') }} AS modalites_accompagnement
    INNER JOIN {{ ref('_map_mediation_numerique__types') }} AS map
        ON modalites_accompagnement.value = map.modalite
    GROUP BY modalites_accompagnement.structure_id
),

publics_raw AS (
    SELECT
        structure_id,
        ARRAY_AGG(value ORDER BY value) AS "publics"
    FROM (
        SELECT
            structure_id,
            value
        FROM {{ ref('stg_mediation_numerique__structures__publics') }}
        UNION ALL
        SELECT
            structure_id,
            value
        FROM {{ ref('stg_mediation_numerique__structures__prises_en_charge') }}
    ) AS combined
    GROUP BY structure_id
),

publics AS (
    SELECT
        structure_id,
        ARRAY_AGG(DISTINCT public ORDER BY public) AS "publics"
    FROM (
        SELECT
            publics.structure_id,
            map.public
        FROM {{ ref('stg_mediation_numerique__structures__publics') }} AS publics
        INNER JOIN {{ ref('_map_mediation_numerique__publics') }} AS map
            ON publics.value = map.public
        UNION ALL
        SELECT
            prises_en_charge.structure_id,
            map.public
        FROM {{ ref('stg_mediation_numerique__structures__prises_en_charge') }} AS prises_en_charge
        INNER JOIN {{ ref('_map_mediation_numerique__publics__prise_en_charge') }} AS map
            ON prises_en_charge.value = map.prise_en_charge
    ) AS combined
    WHERE public IS NOT NULL
    GROUP BY structure_id
),

mediateurs AS (
    SELECT DISTINCT ON (structure_id)
        structure_id,
        NULLIF(TRIM(CONCAT_WS(' ', prenom, nom)), '') AS "contact_nom_prenom",
        email
    FROM {{ ref('stg_mediation_numerique__structures__mediateurs') }}
    ORDER BY structure_id, NULLIF(TRIM(CONCAT_WS(' ', prenom, nom)), '')
),

final AS (
    SELECT
        'mediation-numerique'                                                                        AS "source",
        'mediation-numerique--' || structures.id                                                     AS "id",
        'mediation-numerique--' || structures.id                                                     AS "structure_id",
        'mediation-numerique--' || structures.id                                                     AS "adresse_id",
        CAST(structures.date_maj AS DATE)                                                            AS "date_maj",
        'Service d''accompagnement au numérique'                                                     AS "nom",
        COALESCE(structures.courriel, mediateurs.email)                                              AS "courriel",
        mediateurs.contact_nom_prenom                                                                AS "contact_nom_prenom",
        COALESCE(
            structures.presentation_detail,
            structures.presentation_resume,
            ARRAY_TO_STRING(services_raw.services, ', ')
        )                                                                                            AS "description",
        'https://cartographie.societenumerique.gouv.fr/cartographie/' || structures.id || '/details' AS "lien_source",
        CASE
            WHEN 'aide-materielle' = ANY(types.types) THEN 'aide-materielle'
            WHEN 'atelier' = ANY(types.types) THEN 'atelier'
            WHEN 'accompagnement' = ANY(types.types) THEN 'accompagnement'
        END                                                                                          AS "type",
        thematiques.thematiques                                                                      AS "thematiques",
        frais.frais                                                                                  AS "frais",
        NULL                                                                                         AS "frais_precisions",
        COALESCE(publics.publics, ARRAY['tous-publics'])                                             AS "publics",
        NULLIF(ARRAY_TO_STRING(publics_raw.publics, ', '), '')                                       AS "publics_precisions",
        NULL                                                                                         AS "conditions_acces",
        structures.telephone                                                                         AS "telephone",
        modes_mobilisation.modes_mobilisation                                                        AS "modes_mobilisation",
        structures.prise_rdv                                                                         AS "lien_mobilisation",
        ARRAY['usagers', 'professionnels']                                                           AS "mobilisable_par",
        NULL                                                                                         AS "mobilisation_precisions",
        CASE
            WHEN 'À distance' = ANY(modalites_accompagnement.modalites) THEN ARRAY['a-distance', 'en-presentiel']
            ELSE ARRAY['en-presentiel']
        END                                                                                          AS "modes_accueil",
        CASE
            WHEN structures.adresse__code_insee LIKE '97%' THEN ARRAY[LEFT(structures.adresse__code_insee, 3)]
            WHEN structures.adresse__code_insee IS NOT NULL THEN ARRAY[LEFT(structures.adresse__code_insee, 2)]
        END                                                                                          AS "zone_eligibilite",
        'departement'                                                                                AS "zone_eligibilite_type",
        NULL                                                                                         AS "volume_horaire_hebdomadaire",
        NULL                                                                                         AS "nombre_semaines",
        structures.horaires                                                                          AS "horaires_accueil"
    FROM structures
    LEFT JOIN mediateurs ON structures.id = mediateurs.structure_id
    LEFT JOIN services_raw ON structures.id = services_raw.structure_id
    LEFT JOIN thematiques ON structures.id = thematiques.structure_id
    LEFT JOIN frais ON structures.id = frais.structure_id
    LEFT JOIN modes_mobilisation ON structures.id = modes_mobilisation.structure_id
    LEFT JOIN types ON structures.id = types.structure_id
    LEFT JOIN modalites_accompagnement ON structures.id = modalites_accompagnement.structure_id
    LEFT JOIN publics_raw ON structures.id = publics_raw.structure_id
    LEFT JOIN publics ON structures.id = publics.structure_id
)

SELECT * FROM final
