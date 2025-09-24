WITH lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

services AS (
    SELECT * FROM {{ ref('stg_soliguide__services') }}
),

categories AS (
    SELECT * FROM {{ ref('stg_soliguide__categories') }}
),

phones AS (
    SELECT * FROM {{ ref('stg_soliguide__phones') }}
),

services__publics AS (
    SELECT * FROM {{ ref('stg_soliguide__services__publics') }}
),

filtered_phones AS (
    -- FIXME: di schema only allows a single phone number, but soliguide can have more
    SELECT DISTINCT ON (lieu_id) *
    FROM phones
),

thematiques AS (
    SELECT
        services.id,
        ARRAY_AGG(DISTINCT mapping_.thematique) AS "thematiques"
    FROM
        services
    LEFT JOIN {{ ref('_map_soliguide__thematiques_v1') }} AS mapping_
        ON services.category = mapping_.category
    WHERE mapping_.thematique IS NOT NULL
    GROUP BY services.id
),

publics AS (
    SELECT
        services__publics.lieu_id,
        CASE
            WHEN lieux.publics__accueil IN (0, 1)
                THEN ARRAY['tous-publics']
            ELSE ARRAY_AGG(DISTINCT mapping_.public_datainclusion)
        END AS "publics"
    FROM
        services__publics
    INNER JOIN lieux ON services__publics.lieu_id = lieux.id
    INNER JOIN {{ ref('_map_soliguide__publics_v1') }} AS mapping_
        ON services__publics.value = mapping_.public_soliguide
    WHERE mapping_.public_datainclusion IS NOT NULL
    GROUP BY services__publics.lieu_id, lieux.publics__accueil
),

final AS (
    SELECT
        'soliguide'                                                                           AS "source",
        'soliguide--' || services.id                                                          AS "id",
        'soliguide--' || lieux.id                                                             AS "adresse_id",
        'soliguide--' || lieux.id                                                             AS "structure_id",
        categories.label                                                                      AS "nom",
        ARRAY_TO_STRING(
            ARRAY[
                services.description,
                CASE
                    WHEN services.close__actif AND services.close__date_fin IS NULL
                        THEN 'Ce service est fermé temporairement depuis le ' || TO_CHAR(services.close__date_debut, 'DD/MM/YYYY') || '.'
                    WHEN services.close__actif AND services.close__date_fin IS NOT NULL
                        THEN 'Ce service est fermé temporairement du ' || TO_CHAR(services.close__date_debut, 'DD/MM/YYYY') || ' au ' || TO_CHAR(services.close__date_fin, 'DD/MM/YYYY') || '.'
                END,
                CASE
                    WHEN services.saturated__status = 'HIGH'
                        THEN 'Attention, la structure est très sollicitée pour ce service.'
                END,
                lieux.description
            ],
            E'\n\n'
        )                                                                                     AS "description",
        'https://soliguide.fr/fr/fiche/' || lieux.seo_url                                     AS "lien_source",
        lieux.updated_at                                                                      AS "date_maj",
        mappings_types.type_                                                                  AS "type",
        thematiques.thematiques                                                               AS "thematiques",
        CASE
            WHEN services.modalities__price__checked THEN 'payant'
            ELSE 'gratuit'
        END                                                                                   AS "frais",
        COALESCE(services.modalities__price__precisions, lieux.modalities__price__precisions) AS "frais_precisions",
        publics.publics                                                                       AS "publics",
        ARRAY_TO_STRING(
            ARRAY[
                COALESCE(services.publics__description, lieux.publics__description),
                'L’âge minimum est de ' || lieux.publics__age__min || ' ans.',
                'L’âge maximum est de ' || lieux.publics__age__max || ' ans.'
            ],
            E'\n\n'
        )                                                                                     AS "publics_precisions",
        COALESCE(services.modalities__other, lieux.modalities__other)                         AS "conditions_acces",
        filtered_phones.phone_number                                                          AS "telephone",
        lieux.entity_mail                                                                     AS "courriel",
        NULL                                                                                  AS "contact_nom_prenom",
        ARRAY['en-presentiel']                                                                AS "modes_accueil",
        CAST(NULL AS TEXT [])                                                                 AS "zone_eligibilite",
        'departement'                                                                         AS "zone_eligibilite_type",
        NULL                                                                                  AS "lien_mobilisation",
        ARRAY_REMOVE(
            ARRAY[
                'envoyer-un-courriel',
                'telephoner',
                CASE
                    WHEN services.different_modalities AND services.modalities__inconditionnel
                        THEN 'se-presenter'
                    WHEN lieux.modalities__inconditionnel
                        THEN 'se-presenter'
                END
            ],
            NULL
        )                                                                                     AS "modes_mobilisation",
        ARRAY['usagers', 'professionnels']                                                    AS "mobilisable_par",
        ARRAY_TO_STRING(
            CASE
                WHEN services.different_modalities
                    THEN ARRAY[
                        services.modalities__appointment__precisions,
                        services.modalities__inscription__precisions,
                        services.modalities__orientation__precisions
                    ]
                ELSE ARRAY[
                    lieux.modalities__appointment__precisions,
                    lieux.modalities__inscription__precisions,
                    lieux.modalities__orientation__precisions
                ]
            END,
            E'\n\n'
        )                                                                                     AS "mobilisation_precisions",
        CAST(NULL AS FLOAT)                                                                   AS "volume_horaire_hebdomadaire",
        CAST(NULL AS INTEGER)                                                                 AS "nombre_semaines",
        CASE
            WHEN lieux.temp_infos__closure__actif THEN 'closed'
            WHEN services.different_hours THEN processings.soliguide_opening_hours(services.hours)
            WHEN lieux.temp_infos__hours__actif THEN processings.soliguide_opening_hours(lieux.temp_infos__hours__hours)
            WHEN lieux.newhours IS NOT NULL THEN processings.soliguide_opening_hours(lieux.newhours)
        END                                                                                   AS "horaires_accueil"
    FROM services
    LEFT JOIN lieux ON services.lieu_id = lieux.id
    LEFT JOIN publics ON services.lieu_id = publics.lieu_id
    LEFT JOIN categories ON services.category = categories.code
    LEFT JOIN {{ ref('_map_soliguide__types_v1') }} AS mappings_types ON services.category = mappings_types.category
    LEFT JOIN thematiques ON services.id = thematiques.id
    LEFT JOIN filtered_phones ON services.lieu_id = filtered_phones.lieu_id
)

SELECT * FROM final
