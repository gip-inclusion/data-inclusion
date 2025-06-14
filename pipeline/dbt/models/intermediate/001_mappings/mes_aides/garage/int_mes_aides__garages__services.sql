{% set check_structure_str = "Veuillez vérifier sur le site internet de la structure" %}

WITH garages AS (
    SELECT * FROM {{ ref('stg_mes_aides__garages') }}
),

service_types_by_garage AS (
    SELECT
        garages.id,
        service_type
    FROM garages,
        UNNEST(garages.services) AS service_type
),

final AS (
    SELECT
        garages.id                                                                                  AS "adresse_id",
        TRUE                                                                                        AS "contact_public",
        garages.modifie_le                                                                          AS "date_maj",
        NULL                                                                                        AS "formulaire_en_ligne",
        '{{ check_structure_str }}'                                    AS "frais_autres",
        CAST((UUID(MD5(garages.id || COALESCE(service_types_by_garage.service_type, '')))) AS TEXT) AS "id",
        CAST(NULL AS TEXT [])                                                                       AS "justificatifs",
        'https://mes-aides.francetravail.fr/transport-et-mobilite/garages-solidaires'               AS "lien_source",
        ARRAY['en-presentiel']                                                                      AS "modes_accueil",
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN garages.telephone IS NOT NULL THEN 'telephoner' END,
                CASE WHEN garages.email IS NOT NULL THEN 'envoyer-un-mail' END
            ],
            NULL
        )                                                                                           AS "modes_orientation_accompagnateur",
        CASE
            WHEN
                garages.telephone IS NULL
                AND garages.email IS NULL
                THEN '{{ check_structure_str }}'
        END                                                                                         AS "modes_orientation_accompagnateur_autres",
        ARRAY_REMOVE(
            ARRAY[
                'se-presenter',
                CASE WHEN garages.telephone IS NOT NULL THEN 'telephoner' END,
                CASE WHEN garages.email IS NOT NULL THEN 'envoyer-un-mail' END
            ],
            NULL
        )                                                                                           AS "modes_orientation_beneficiaire",
        NULL                                                                                        AS "modes_orientation_beneficiaire_autres",
        FORMAT(
            '%s de %s',
            COALESCE(service_types_by_garage.service_type, 'Réparation, vente et location'),
            LOWER(COALESCE(NULLIF(ARRAY_TO_STRING(garages.types_de_vehicule, ', '), ''), 'véhicule'))
        )                                                                                           AS "nom",
        FORMAT(
            '%s de %s à tarif solidaire pour les personnes en difficulté, et selon leur situation',
            COALESCE(service_types_by_garage.service_type, 'Réparation, vente et location'),
            LOWER(COALESCE(NULLIF(ARRAY_TO_STRING(garages.types_de_vehicule, ', '), ''), 'véhicule'))
        )                                                                                           AS "presentation_resume",
        NULL                                                                                        AS "presentation_detail",
        NULL                                                                                        AS "prise_rdv",
        CAST(NULL AS TEXT [])                                                                       AS "profils",
        LEFT(garages.criteres_eligibilite_raw, 500)                                                 AS "profils_precisions",
        NULL                                                                                        AS "recurrence",
        garages._di_source_id                                                                       AS "source",
        garages.id                                                                                  AS "structure_id",
        ARRAY_REMOVE(
            ARRAY[
                CASE service_types_by_garage.service_type
                    WHEN 'Achat' THEN 'mobilite--acheter-un-vehicule-motorise'
                    WHEN 'Location' THEN 'mobilite--louer-un-vehicule'
                    WHEN 'Réparation' THEN 'mobilite--entretenir-reparer-son-vehicule'
                    ELSE 'mobilite'
                END,
                CASE
                    WHEN
                        service_types_by_garage.service_type = 'Achat'
                        AND 'Vélo' = ANY(garages.types_de_vehicule)
                        THEN 'mobilite--acheter-un-velo'
                END
            ],
            NULL
        )                                                                                           AS "thematiques",
        ARRAY['aide-materielle', 'aide-financiere']                                                 AS "types",
        CASE LEFT(garages.code_insee, 2)
            WHEN '97' THEN LEFT(garages.code_insee, 3)
            ELSE LEFT(garages.code_insee, 2)
        END                                                                                         AS "zone_diffusion_code",
        NULL                                                                                        AS "zone_diffusion_nom",
        'departement'                                                                               AS "zone_diffusion_type",
        garages.criteres_eligibilite                                                                AS "pre_requis",
        NULL                                                                                        AS "contact_nom_prenom",
        garages.email                                                                               AS "courriel",
        SUBSTRING(garages.telephone FROM '\+?\d[\d\.\-\s]*\d')                                      AS "telephone",
        CAST(NULL AS TEXT [])                                                                       AS "frais",
        NULL                                                                                        AS "page_web"
    FROM garages
    LEFT JOIN service_types_by_garage ON garages.id = service_types_by_garage.id
    WHERE
        garages.en_ligne
)

SELECT * FROM final
