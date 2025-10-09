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

publics AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux__publics__administrative') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_soliguide__lieux__publics__gender') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_soliguide__lieux__publics__familiale') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_soliguide__lieux__publics__other') }}
),

mapping_thematiques AS (
    SELECT * FROM {{ ref('_map_soliguide__thematiques') }}
),

mapping_types AS (
    SELECT * FROM {{ ref('_map_soliguide__types') }}
),

profils AS (
    SELECT
        publics.lieu_id,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT di_mapping.traduction), ', ') AS traduction,
        ARRAY_REMOVE(ARRAY_AGG(DISTINCT di_mapping.profils), NULL)       AS profils
    FROM
        publics
    LEFT JOIN (
        VALUES
        -- administrative status
        ('regular', 'en situation régulière', NULL),
        ('asylum', 'demandeur asile', 'personnes-de-nationalite-etrangere'),
        ('refugee', 'personne avec un status de refugiée', 'personnes-de-nationalite-etrangere'),
        ('undocumented', 'sans-papiers', 'personnes-de-nationalite-etrangere'),
        -- family status
        ('isolated', 'isolé', NULL),
        ('family', 'famille', 'familles-enfants'),
        ('couple', 'couple', 'familles-enfants'),
        ('pregnant', 'enceinte', 'familles-enfants'),
        -- gender status
        ('men', 'homme', NULL),
        ('women', 'femme', 'femmes'),
        -- other status
        ('violence', 'victime de violence', 'victimes'),
        ('addiction', 'personne en situation d''addiction', 'personnes-en-situation-durgence'),
        ('handicap', 'personne en situation d''handicap', 'personnes-en-situation-de-handicap'),
        ('lgbt', 'personne LGBT+', NULL),
        ('hiv', 'vih personne séropositive', NULL),
        ('prostitution', 'personne en situation de prostitution', NULL),
        ('prison', 'personne sortant de prison', 'sortants-de-detention'),
        ('student', 'étudiant', 'etudiants'),
        ('ukraine', 'ukraine', 'personnes-de-nationalite-etrangere')
        -- ('mentalHealth', NULL, NULL)
    ) AS di_mapping (category, traduction, profils) ON publics.value = di_mapping.category
    GROUP BY
        publics.lieu_id
),

filtered_phones AS (
    -- FIXME: di schema only allows a single phone number, but soliguide can have more
    SELECT DISTINCT ON (lieu_id) *
    FROM phones
),

services_with_description AS (
    SELECT
        services.*,
        CASE
            WHEN LENGTH(services.description) <= 280 THEN services.description
            ELSE LEFT(services.description, 279) || '…'
        END AS "description_courte",
        CASE
            WHEN services.close__actif AND services.close__date_fin IS NULL
                THEN 'Ce service est fermé temporairement depuis le ' || TO_CHAR(services.close__date_debut, 'DD/MM/YYYY') || E'.\n'
            WHEN services.close__actif AND services.close__date_fin IS NOT NULL
                THEN 'Ce service est fermé temporairement du ' || TO_CHAR(services.close__date_debut, 'DD/MM/YYYY') || ' au ' || TO_CHAR(services.close__date_fin, 'DD/MM/YYYY') || E'.\n'
            ELSE ''
        END || CASE
            WHEN services.saturated__status = 'HIGH'
                THEN E'Attention, la structure est très sollicitée pour ce service.\n'
            ELSE ''
        END || COALESCE(services.description, lieux.description) || CASE
            WHEN services.hours ->> 'closedHolidays' = 'CLOSED'
                THEN E'\nLa structure est fermée pendant les jours fériés.'
            ELSE ''
        END AS "description_longue"
    FROM services
    LEFT JOIN lieux ON services.lieu_id = lieux.id
),

open_services AS (
    SELECT *
    FROM services_with_description
    WHERE
        NOT close__actif
        OR
        (close__date_debut > CURRENT_DATE AT TIME ZONE 'Europe/Paris')
        OR
        (close__date_fin < CURRENT_DATE AT TIME ZONE 'Europe/Paris')
),

final AS (
    SELECT
        open_services.id                                  AS "id",
        lieux.lieu_id                                     AS "adresse_id",
        'soliguide'                                       AS "source",
        CASE
            WHEN mapping_types.di_type IS NULL THEN NULL
            ELSE ARRAY[mapping_types.di_type]
        END                                               AS "types",
        NULL                                              AS "prise_rdv",
        NULL                                              AS "lien_mobilisation",
        CASE
            WHEN lieux.publics__accueil IN (0, 1) THEN ARRAY_APPEND(profils.profils, 'tous-publics')
            ELSE profils.profils
        END                                               AS "profils",
        profils.traduction                                AS "profils_precisions",
        CAST(NULL AS TEXT [])                             AS "pre_requis",
        CAST(NULL AS TEXT [])                             AS "justificatifs",
        NULL                                              AS "conditions_acces",
        filtered_phones.phone_number                      AS "telephone",
        lieux.entity_mail                                 AS "courriel",
        NULL                                              AS "contact_nom_prenom",
        open_services.updated_at                          AS "date_maj",
        NULL                                              AS "page_web",
        'commune'                                         AS "zone_diffusion_type",
        NULL                                              AS "zone_diffusion_code",  -- will be overridden after geocoding
        NULL                                              AS "zone_diffusion_nom",  -- will be overridden after geocoding
        CAST(NULL AS FLOAT)                               AS "volume_horaire_hebdomadaire",
        CAST(NULL AS INT)                                 AS "nombre_semaines",
        NULL                                              AS "formulaire_en_ligne",
        open_services.lieu_id                             AS "structure_id",
        ARRAY[mapping_thematiques.thematique]             AS "thematiques",
        ARRAY['en-presentiel']                            AS "modes_accueil",
        CAST(NULL AS TEXT [])                             AS "modes_mobilisation",
        CAST(NULL AS TEXT [])                             AS "mobilisable_par",
        NULL                                              AS "mobilisation_precisions",
        categories.label                                  AS "nom",
        'https://soliguide.fr/fr/fiche/' || lieux.seo_url AS "lien_source",
        open_services.description_courte                  AS "presentation_resume",
        open_services.description_longue                  AS "presentation_detail",
        CASE
            WHEN open_services.modalities__price__checked THEN ARRAY['payant']
            ELSE ARRAY['gratuit']
        END                                               AS "frais",
        open_services.modalities__price__precisions       AS "frais_autres",
        CASE
            WHEN open_services.different_hours
                THEN UDF_SOLIGUIDE__NEW_HOURS_TO_OSM_OPENING_HOURS(open_services.hours)
            ELSE UDF_SOLIGUIDE__NEW_HOURS_TO_OSM_OPENING_HOURS(lieux.newhours)
        END                                               AS "recurrence",
        ARRAY_REMOVE(
            ARRAY[
                CASE
                    WHEN
                        open_services.modalities__inconditionnel
                        OR open_services.modalities__appointment__checked
                        OR open_services.modalities__inscription__checked
                        OR open_services.modalities__orientation__checked
                        THEN 'telephoner'
                END,
                CASE
                    WHEN
                        open_services.modalities__appointment__checked
                        OR open_services.modalities__inscription__checked
                        OR open_services.modalities__orientation__checked
                        THEN 'envoyer-un-mail'
                END,
                CASE WHEN open_services.modalities__orientation__checked THEN 'envoyer-un-mail-avec-une-fiche-de-prescription' END
            ],
            NULL
        )                                                 AS "modes_orientation_accompagnateur",
        ARRAY_TO_STRING(
            ARRAY[
                CASE WHEN open_services.modalities__appointment__checked THEN '## Sur rendez-vous :' || E'\n' || open_services.modalities__appointment__precisions END,
                CASE WHEN open_services.modalities__inscription__checked THEN '## Sur inscription :' || E'\n' || open_services.modalities__inscription__precisions END,
                CASE WHEN open_services.modalities__orientation__checked THEN '## Sur orientation :' || E'\n' || open_services.modalities__orientation__precisions END
            ],
            E'\n\n'
        )                                                 AS "modes_orientation_accompagnateur_autres",
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN (open_services.modalities__inconditionnel OR open_services.modalities__inscription__checked) AND lieux.position__address IS NOT NULL THEN 'se-presenter' END,
                CASE WHEN open_services.modalities__appointment__checked OR open_services.modalities__inscription__checked THEN 'telephoner' END,
                CASE WHEN open_services.modalities__appointment__checked THEN 'envoyer-un-mail' END,
                CASE WHEN open_services.modalities__orientation__checked THEN 'autre' END
            ],
            NULL
        )                                                 AS "modes_orientation_beneficiaire",
        ARRAY_TO_STRING(
            ARRAY[
                CASE WHEN open_services.modalities__orientation__checked THEN '## Orientation par un professionnel' END,
                CASE WHEN open_services.modalities__appointment__checked THEN '## Sur rendez-vous :' || E'\n' || open_services.modalities__appointment__precisions END,
                CASE WHEN open_services.modalities__inscription__checked THEN '## Sur inscription :' || E'\n' || open_services.modalities__inscription__precisions END
            ],
            E'\n\n'
        )                                                 AS "modes_orientation_beneficiaire_autres"
    FROM open_services
    LEFT JOIN lieux ON open_services.lieu_id = lieux.id
    LEFT JOIN categories ON open_services.category = categories.code
    LEFT JOIN filtered_phones ON open_services.lieu_id = filtered_phones.lieu_id
    LEFT JOIN profils ON lieux.id = profils.lieu_id
    LEFT JOIN mapping_types ON open_services.category = mapping_types.category
    LEFT JOIN mapping_thematiques ON open_services.category = mapping_thematiques.category
    -- remove services without mapped thematiques which are assumed irrelevant
    WHERE mapping_thematiques.thematique IS NOT NULL
    ORDER BY open_services.id
)

SELECT * FROM final
