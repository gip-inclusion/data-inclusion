{% set check_structure_str = "Veuillez vérifier sur le site internet de la structure" %}

WITH garages AS (
    SELECT * FROM {{ ref('stg_mes_aides__garages') }}
),

garages__services AS (
    SELECT * FROM {{ ref('stg_mes_aides__garages__services') }}
),

garages__types_de_vehicule AS (
    SELECT * FROM {{ ref('stg_mes_aides__garages__types_de_vehicule') }}
),

final AS (
    SELECT
        garages.id                                                                    AS "adresse_id",
        garages.modifie_le                                                            AS "date_maj",
        NULL                                                                          AS "formulaire_en_ligne",
        '{{ check_structure_str }}'                      AS "frais_autres",
        CAST((UUID(MD5(garages.id || COALESCE(garages__services.item, '')))) AS TEXT) AS "id",
        CAST(NULL AS TEXT [])                                                         AS "justificatifs",
        'https://mes-aides.francetravail.fr/transport-et-mobilite/garages-solidaires' AS "lien_source",
        ARRAY['en-presentiel']                                                        AS "modes_accueil",
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN garages.telephone IS NOT NULL THEN 'telephoner' END,
                CASE WHEN garages.email IS NOT NULL THEN 'envoyer-un-mail' END
            ],
            NULL
        )                                                                             AS "modes_orientation_accompagnateur",
        CASE
            WHEN
                garages.telephone IS NULL
                AND garages.email IS NULL
                THEN '{{ check_structure_str }}'
        END                                                                           AS "modes_orientation_accompagnateur_autres",
        ARRAY_REMOVE(
            ARRAY[
                'se-presenter',
                CASE WHEN garages.telephone IS NOT NULL THEN 'telephoner' END,
                CASE WHEN garages.email IS NOT NULL THEN 'envoyer-un-mail' END
            ],
            NULL
        )                                                                             AS "modes_orientation_beneficiaire",
        NULL                                                                          AS "modes_orientation_beneficiaire_autres",
        CAST(NULL AS TEXT [])                                                         AS "modes_mobilisation",
        CAST(NULL AS TEXT [])                                                         AS "mobilisable_par",
        NULL                                                                          AS "mobilisation_precisions",
        FORMAT(
            '%s de %s',
            COALESCE(garages__services.item, 'Réparation, vente et location'),
            LOWER(COALESCE(NULLIF(ARRAY_TO_STRING(garages__types_de_vehicule.items, ', '), ''), 'véhicule'))
        )                                                                             AS "nom",
        FORMAT(
            '%s de %s à tarif solidaire pour les personnes en difficulté, et selon leur situation',
            COALESCE(garages__services.item, 'Réparation, vente et location'),
            LOWER(COALESCE(NULLIF(ARRAY_TO_STRING(garages__types_de_vehicule.items, ', '), ''), 'véhicule'))
        )                                                                             AS "presentation_resume",
        NULL                                                                          AS "presentation_detail",
        NULL                                                                          AS "prise_rdv",
        NULL                                                                          AS "lien_mobilisation",
        CAST(NULL AS TEXT [])                                                         AS "profils",
        LEFT(garages.criteres_eligibilite, 500)                                       AS "profils_precisions",
        NULL                                                                          AS "recurrence",
        'mes-aides'                                                                   AS "source",
        garages.id                                                                    AS "structure_id",
        ARRAY_REMOVE(
            ARRAY[
                CASE garages__services.item
                    WHEN 'Achat' THEN 'mobilite--acheter-un-vehicule-motorise'
                    WHEN 'Location' THEN 'mobilite--louer-un-vehicule'
                    WHEN 'Réparation' THEN 'mobilite--entretenir-reparer-son-vehicule'
                    ELSE 'mobilite'
                END,
                CASE
                    WHEN
                        garages__services.item = 'Achat'
                        AND 'Vélo' = ANY(garages__types_de_vehicule.items)
                        THEN 'mobilite--acheter-un-velo'
                END
            ],
            NULL
        )                                                                             AS "thematiques",
        ARRAY['aide-materielle', 'aide-financiere']                                   AS "types",
        CASE LEFT(garages.code_insee, 2)
            WHEN '97' THEN LEFT(garages.code_insee, 3)
            ELSE LEFT(garages.code_insee, 2)
        END                                                                           AS "zone_diffusion_code",
        NULL                                                                          AS "zone_diffusion_nom",
        'departement'                                                                 AS "zone_diffusion_type",
        CAST(NULL AS FLOAT)                                                           AS "volume_horaire_hebdomadaire",
        CAST(NULL AS INT)                                                             AS "nombre_semaines",
        CASE
            WHEN garages.criteres_eligibilite IS NOT NULL
                THEN ARRAY_REMOVE(
                    ARRAY(
                        SELECT (REGEXP_MATCHES(fields ->> 'Critères d''éligibilité', '^\W (.*?)( )*?$', 'gn'))[1]
                    ),
                    ''
                )
        END                                                                           AS "pre_requis",
        NULL                                                                          AS "contact_nom_prenom",
        garages.email                                                                 AS "courriel",
        SUBSTRING(garages.telephone FROM '\+?\d[\d\.\-\s]*\d')                        AS "telephone",
        CAST(NULL AS TEXT [])                                                         AS "frais",
        NULL                                                                          AS "page_web"
    FROM garages
    LEFT JOIN garages__services ON garages.id = garages__services.garage_id
    LEFT JOIN garages__types_de_vehicule ON garages.id = garages__types_de_vehicule.garage_id
    WHERE
        garages.en_ligne
)

SELECT * FROM final
