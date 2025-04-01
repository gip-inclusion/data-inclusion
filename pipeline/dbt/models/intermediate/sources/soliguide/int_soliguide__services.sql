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

thematiques AS (
    SELECT * FROM {{ ref('thematiques') }}
),

di_thematique_by_soliguide_category_code AS (
    SELECT x.*
    FROM (
        VALUES
        -- Mapping: https://grist.incubateur.net/o/datainclusion/mDqdLZVGVmER/Mappings/p/4
        ('access_to_housing', ARRAY['logement-hebergement--etre-accompagne-pour-se-loger']),
        ('accomodation_and_housing', ARRAY['logement-hebergement']),
        ('addiction', ARRAY['sante--faire-face-a-une-situation-daddiction']),
        ('administrative_assistance', ARRAY['acces-aux-droits-et-citoyennete--connaitre-ses-droits', 'acces-aux-droits-et-citoyennete--accompagnement-dans-les-demarches-administratives']),
        ('baby_parcel', ARRAY['famille--soutien-a-la-parentalite']),
        ('babysitting', ARRAY['famille--garde-denfants']),
        ('budget_advice', ARRAY(
            SELECT thematiques.value FROM thematiques
            WHERE thematiques.value ~ '^gestion-financiere--'
        )),
        ('carpooling', ARRAY['mobilite--comprendre-et-utiliser-les-transports-en-commun']),
        ('chauffeur_driven_transport', ARRAY['mobilite--comprendre-et-utiliser-les-transports-en-commun']),
        ('child_care', ARRAY['sante--accompagnement-de-la-femme-enceinte-du-bebe-et-du-jeune-enfant']),
        ('citizen_housing', ARRAY['logement-hebergement']),
        ('clothing', ARRAY['equipement-et-alimentation--habillement']),
        ('computers_at_your_disposal', ARRAY['numerique--acceder-a-du-materiel']),
        ('cooking_workshop', ARRAY['equipement-et-alimentation--alimentation']),
        ('day_hosting', ARRAY['remobilisation--lien-social']),
        ('dental_care', ARRAY['sante--acces-aux-soins']),
        ('digital_tools_training', ARRAY(
            SELECT thematiques.value FROM thematiques
            WHERE thematiques.value ~ '^numerique--'
        )),
        ('disability_advice', ARRAY['handicap']),
        ('domiciliation', ARRAY['acces-aux-droits-et-citoyennete--accompagnement-dans-les-demarches-administratives']),
        ('emergency_accommodation', ARRAY['logement-hebergement--mal-loges-sans-logis']),
        ('family_area', ARRAY['famille--soutien-a-la-parentalite']),
        ('fountain', ARRAY['equipement-et-alimentation--alimentation']),
        ('food_distribution', ARRAY['equipement-et-alimentation--alimentation']),
        ('food_packages', ARRAY['equipement-et-alimentation--alimentation']),
        ('food_voucher', ARRAY['equipement-et-alimentation--alimentation']),
        ('food', ARRAY['equipement-et-alimentation--alimentation']),
        ('french_course', ARRAY['apprendre-francais--suivre-formation']),
        ('general_practitioner', ARRAY['sante--acces-aux-soins']),
        ('health', ARRAY['sante']),
        ('health_specialists', ARRAY['sante--acces-aux-soins']),
        ('infirmary', ARRAY['sante--acces-aux-soins']),
        ('integration_through_economic_activity', ARRAY['accompagnement-social-et-professionnel-personnalise--parcours-d-insertion-socioprofessionnel']),
        ('job_coaching', ARRAY['trouver-un-emploi']),
        ('legal_advice', ARRAY['acces-aux-droits-et-citoyennete--accompagnement-juridique']),
        ('libraries', ARRAY['remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture']),
        ('long_term_accomodation', ARRAY['logement-hebergement--mal-loges-sans-logis']),
        ('mobility', ARRAY['mobilite']),
        ('mobility_assistance', ARRAY['mobilite--etre-accompagne-dans-son-parcours-mobilite']),
        ('museums', ARRAY['remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture']),
        ('other_activities', ARRAY['remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture']),
        ('overnight_stop', ARRAY['logement-hebergement--mal-loges-sans-logis']),
        ('parent_assistance', ARRAY['famille--information-et-accompagnement-des-parents', 'famille--soutien-a-la-parentalite', 'famille--soutien-aux-familles']),
        ('pregnancy_care', ARRAY['sante--accompagnement-de-la-femme-enceinte-du-bebe-et-du-jeune-enfant']),
        ('provision_of_vehicles', ARRAY['mobilite--louer-un-vehicule']),
        ('psychological_support', ARRAY['sante--bien-etre-psychologique']),
        ('public_writer', ARRAY['acces-aux-droits-et-citoyennete--accompagnement-dans-les-demarches-administratives', 'numerique--realiser-des-demarches-administratives-avec-un-accompagnement']),
        ('seated_catering', ARRAY['equipement-et-alimentation--alimentation']),
        ('shared_kitchen', ARRAY['equipement-et-alimentation--alimentation']),
        ('social_accompaniment', ARRAY['accompagnement-social-et-professionnel-personnalise']),
        ('social_grocery_stores', ARRAY['equipement-et-alimentation--alimentation']),
        ('solidarity_fridge', ARRAY['equipement-et-alimentation--alimentation']),
        ('solidarity_store', ARRAY['equipement-et-alimentation--habillement']),
        ('sport_activities', ARRAY['remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture']),
        ('std_testing', ARRAY['sante--prevention-et-acces-aux-soins']),
        ('telephone_at_your_disposal', ARRAY['equipement-et-alimentation--acces-a-un-telephone-et-un-abonnement']),
        ('tutoring', ARRAY['trouver-un-emploi']),
        ('vaccination', ARRAY['sante--prevention-et-acces-aux-soins']),
        ('wellness', ARRAY['remobilisation--bien-etre']),
        ('wifi', ARRAY['numerique--acceder-a-une-connexion-internet'])
    ) AS x (category, thematique)
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
    ) AS di_mapping (category, traduction, profils) ON publics.value = di_mapping.category
    GROUP BY
        publics.lieu_id
),

filtered_phones AS (
    -- FIXME: di schema only allows a single phone number, but soliguide can have more
    SELECT DISTINCT ON (lieu_id) *
    FROM phones
),

-- remove services without mapped thematiques which are assumed irrelevant
relevant_services AS (
    SELECT *
    FROM services
    WHERE category IN (
        SELECT c.category FROM di_thematique_by_soliguide_category_code AS c
    )
),

-- remove temporarily suspended services from downstream data
-- FIXME: these services should ideally be in the downstream but flagged as unavailable in some way
open_services AS (
    SELECT *
    FROM relevant_services
    WHERE
        NOT close__actif
        OR
        (close__date_debut IS NOT NULL OR close__date_fin IS NOT NULL)
        AND
        /* Support for OVERLAPS clause with postgres engine is not broad with DBT.
           It is getting better but we're not there yet.
           https://github.com/sqlfluff/sqlfluff/issues/4664
        */
        -- noqa: disable=PRS
        (
            CURRENT_DATE AT TIME ZONE 'Europe/Paris',
            CURRENT_DATE AT TIME ZONE 'Europe/Paris'
        )
        OVERLAPS  -- noqa: LT02
        (
            COALESCE(close__date_debut, CURRENT_DATE - INTERVAL '1 year'),
            COALESCE(close__date_fin, CURRENT_DATE + INTERVAL '1 year')
        )  -- noqa: enable=PRS
),


-- TODO(vmttn): clean up modes_orientation_* with dbt macros ?

final AS (
    SELECT
        open_services.id                                              AS "id",
        lieux.lieu_id                                                 AS "adresse_id",
        open_services._di_source_id                                   AS "source",
        CAST(NULL AS TEXT [])                                         AS "types",
        NULL                                                          AS "prise_rdv",
        CASE
            WHEN lieux.publics__accueil IN (0, 1) THEN ARRAY_APPEND(profils.profils, 'tous-publics')
            ELSE profils.profils
        END                                                           AS "profils",
        profils.traduction                                            AS "profils_precisions",
        CAST(NULL AS TEXT [])                                         AS "pre_requis",
        TRUE                                                          AS "cumulable",
        CAST(NULL AS TEXT [])                                         AS "justificatifs",
        CAST(NULL AS DATE)                                            AS "date_creation",
        CAST(NULL AS DATE)                                            AS "date_suspension",
        filtered_phones.phone_number                                  AS "telephone",
        lieux.entity_mail                                             AS "courriel",
        CAST(NULL AS BOOLEAN)                                         AS "contact_public",
        NULL                                                          AS "contact_nom_prenom",
        open_services.updated_at                                      AS "date_maj",
        NULL                                                          AS "page_web",
        'commune'                                                     AS "zone_diffusion_type",
        NULL                                                          AS "zone_diffusion_code",  -- will be overridden after geocoding
        NULL                                                          AS "zone_diffusion_nom",  -- will be overridden after geocoding
        NULL                                                          AS "formulaire_en_ligne",
        open_services.lieu_id                                         AS "structure_id",
        CAST((
            SELECT di_thematique_by_soliguide_category_code.thematique
            FROM di_thematique_by_soliguide_category_code
            WHERE open_services.category = di_thematique_by_soliguide_category_code.category
        ) AS TEXT [])                                                 AS "thematiques",
        ARRAY['en-presentiel']                                        AS "modes_accueil",
        categories.label || COALESCE(' : ' || open_services.name, '') AS "nom",
        'https://soliguide.fr/fr/fiche/' || lieux.seo_url             AS "lien_source",
        CASE
            WHEN LENGTH(open_services.description) <= 280 THEN open_services.description
            ELSE LEFT(open_services.description, 279) || '…'
        END                                                           AS "presentation_resume",
        CASE
            WHEN LENGTH(open_services.description) <= 280 THEN NULL
            ELSE open_services.description
        END                                                           AS "presentation_detail",
        CASE
            WHEN open_services.modalities__price__checked THEN ARRAY['payant']
            ELSE ARRAY['gratuit']
        END                                                           AS "frais",
        open_services.modalities__price__precisions                   AS "frais_autres",
        CASE
            WHEN open_services.different_hours
                THEN UDF_SOLIGUIDE__NEW_HOURS_TO_OSM_OPENING_HOURS(open_services.hours)
            ELSE UDF_SOLIGUIDE__NEW_HOURS_TO_OSM_OPENING_HOURS(lieux.newhours)
        END                                                           AS "recurrence",
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
        )                                                             AS "modes_orientation_accompagnateur",
        ARRAY_TO_STRING(
            ARRAY[
                CASE WHEN open_services.modalities__appointment__checked THEN '## Sur rendez-vous :' || E'\n' || open_services.modalities__appointment__precisions END,
                CASE WHEN open_services.modalities__inscription__checked THEN '## Sur inscription :' || E'\n' || open_services.modalities__inscription__precisions END,
                CASE WHEN open_services.modalities__orientation__checked THEN '## Sur orientation :' || E'\n' || open_services.modalities__orientation__precisions END
            ],
            E'\n\n'
        )                                                             AS "modes_orientation_accompagnateur_autres",
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN (open_services.modalities__inconditionnel OR open_services.modalities__inscription__checked) AND lieux.position__address IS NOT NULL THEN 'se-presenter' END,
                CASE WHEN open_services.modalities__appointment__checked OR open_services.modalities__inscription__checked THEN 'telephoner' END,
                CASE WHEN open_services.modalities__appointment__checked THEN 'envoyer-un-mail' END,
                CASE WHEN open_services.modalities__orientation__checked THEN 'autre' END
            ],
            NULL
        )                                                             AS "modes_orientation_beneficiaire",
        ARRAY_TO_STRING(
            ARRAY[
                CASE WHEN open_services.modalities__orientation__checked THEN '## Orientation par un professionnel' END,
                CASE WHEN open_services.modalities__appointment__checked THEN '## Sur rendez-vous :' || E'\n' || open_services.modalities__appointment__precisions END,
                CASE WHEN open_services.modalities__inscription__checked THEN '## Sur inscription :' || E'\n' || open_services.modalities__inscription__precisions END
            ],
            E'\n\n'
        )                                                             AS "modes_orientation_beneficiaire_autres"
    FROM open_services
    LEFT JOIN lieux ON open_services.lieu_id = lieux.id
    LEFT JOIN categories ON open_services.category = categories.code
    LEFT JOIN filtered_phones ON open_services.lieu_id = filtered_phones.lieu_id
    LEFT JOIN profils AS profils ON lieux.id = profils.lieu_id
    ORDER BY open_services.id
)

SELECT * FROM final
