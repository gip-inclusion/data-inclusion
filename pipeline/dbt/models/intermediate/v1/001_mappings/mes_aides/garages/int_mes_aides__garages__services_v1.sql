WITH garages AS (
    SELECT * FROM {{ ref('stg_mes_aides__garages') }}
),

garages__besoins AS (
    SELECT DISTINCT ON (garage_id) *
    FROM {{ ref('stg_mes_aides__garages__besoins') }}
    ORDER BY garage_id ASC, item ASC
),

garages__services AS (
    SELECT
        garage_id,
        ARRAY_AGG(item) AS items
    FROM {{ ref('stg_mes_aides__garages__services') }}
    WHERE item = ANY(ARRAY['location', 'achat', 'reparation'])
    GROUP BY garage_id
),

garages__types_de_vehicule AS (
    SELECT
        garage_id,
        ARRAY_AGG(item) AS items
    FROM {{ ref('stg_mes_aides__garages__types_de_vehicule') }}
    GROUP BY garage_id
),

final AS (
    SELECT
        'mes-aides'                                         AS "source",
        'mes-aides--' || garages.id                         AS "id",
        'mes-aides--' || garages.id                         AS "adresse_id",
        'mes-aides--' || garages.id                         AS "structure_id",
        FORMAT(
            '%s de %s',
            COALESCE(ARRAY_TO_STRING(garages__services.items, ', '), 'Réparation, vente et location'),
            LOWER(COALESCE(NULLIF(ARRAY_TO_STRING(garages__types_de_vehicule.items, ', '), ''), 'véhicules'))
        )                                                   AS "nom",
        FORMAT(
            '%s de %s à tarif solidaire pour les personnes en difficulté, et selon leur situation.',
            COALESCE(ARRAY_TO_STRING(garages__services.items, ', '), 'Réparation, vente et location'),
            LOWER(COALESCE(NULLIF(ARRAY_TO_STRING(garages__types_de_vehicule.items, ', '), ''), 'véhicule'))
        )                                                   AS "description",
        FORMAT(
            'https://mes-aides.francetravail.fr/%s/%s/%s/%s',
            REGEXP_REPLACE(LOWER(UNACCENT(garages__besoins.categorie)), '\W', '-', 'g'),
            REGEXP_REPLACE(LOWER(UNACCENT(garages__besoins.item)), '\W', '-', 'g'),
            REGEXP_REPLACE(LOWER(UNACCENT(garages.partenaire)), '\W', '-', 'g'),
            REGEXP_REPLACE(LOWER(UNACCENT(garages.nom)), '\W', '-', 'g')
        )                                                   AS "lien_source",
        COALESCE(garages.mis_a_jour_le, garages.modifie_le) AS "date_maj",
        'aide-materielle'                                   AS "type",
        ARRAY_REMOVE(
            ARRAY[
                CASE
                    WHEN
                        'achat' = ANY(garages__services.items)
                        OR 'location' = ANY(garages__services.items)
                        OR garages__services.items IS NULL
                        THEN 'mobilite--acceder-a-un-vehicule'
                END,
                CASE
                    WHEN
                        'reparation' = ANY(garages__services.items)
                        OR garages__services.items IS NULL
                        THEN 'mobilite--entretenir-reparer-son-vehicule'
                END
            ],
            NULL
        )                                                   AS "thematiques",
        NULL                                                AS "frais",
        NULL                                                AS "frais_precisions",
        ARRAY['tous-publics']                               AS "publics",
        NULL                                                AS "publics_precisions",
        garages.criteres_eligibilite                        AS "conditions_acces",
        garages.telephone                                   AS "telephone",
        garages.email                                       AS "courriel",
        NULL                                                AS "contact_nom_prenom",
        ARRAY['en-presentiel']                              AS "modes_accueil",
        ARRAY[garages.departement__code]                    AS "zone_eligibilite",
        'departement'                                       AS "zone_eligibilite_type",
        NULL                                                AS "lien_mobilisation",
        ARRAY_REMOVE(
            ARRAY[
                'se-presenter',
                CASE WHEN garages.telephone IS NOT NULL THEN 'telephoner' END,
                CASE WHEN garages.email IS NOT NULL THEN 'envoyer-un-courriel' END
            ],
            NULL
        )                                                   AS "modes_mobilisation",
        ARRAY['usagers', 'professionnels']                  AS "mobilisable_par",
        NULL                                                AS "mobilisation_precisions",
        NULL                                                AS "volume_horaire_hebdomadaire",
        NULL                                                AS "nombre_semaines",
        NULL                                                AS "horaires_accueil"
    FROM garages
    LEFT JOIN garages__besoins AS garages__besoins ON garages.id = garages__besoins.garage_id
    LEFT JOIN garages__services AS garages__services ON garages.id = garages__services.garage_id
    LEFT JOIN garages__types_de_vehicule AS garages__types_de_vehicule ON garages.id = garages__types_de_vehicule.garage_id
    WHERE
        garages.en_ligne
)

SELECT * FROM final
