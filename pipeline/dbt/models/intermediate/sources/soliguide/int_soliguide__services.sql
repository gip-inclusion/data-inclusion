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

thematiques AS (
    SELECT * FROM {{ ref('thematiques') }}
),

di_thematique_by_soliguide_categorie_code AS (
    SELECT x.*
    FROM (
        VALUES
        -- Mapping: https://grist.incubateur.net/o/datainclusion/mDqdLZVGVmER/Mappings/p/4
        ('access_to_housing', ARRAY['logement-hebergement--etre-accompagne-pour-se-loger'])
        ('accomodation_and_housing', ARRAY['logement-hebergement']),
        ('addiction', ARRAY['sante--faire-face-a-une-situation-daddiction']),
        ('administrative_assistance', ARRAY['acces-aux-droits-et-citoyennete--connaitre-ses-droits', 'acces-aux-droits-et-citoyennete--accompagnement-dans-les-demarches-administratives']),
        ('babysitting', ARRAY['famille--garde-denfants']),
        ('budget_advice', ARRAY(SELECT value FROM thematiques WHERE value ~ '^gestion-financiere--')),
        ('carpooling', ARRAY['mobilite--comprendre-et-utiliser-les-transports-en-commun']),
        ('chauffeur_driven_transport', ARRAY['mobilite--comprendre-et-utiliser-les-transports-en-commun']),
        ('clothing', ARRAY['equipement-et-alimentation--habillement']),
        ('computers_at_your_disposal', ARRAY['numerique--acceder-a-du-materiel']),
        ('day_hosting', ARRAY['remobilisation--lien-social']),
        ('digital_tools_training', ARRAY(SELECT value FROM thematiques WHERE value ~ '^numerique--')),
        ('emergency_accommodation', ARRAY['logement-hebergement--mal-loges-sans-logis']),
        ('family_area', ARRAY['famille--soutien-a-la-parentalite']),
        ('food_distribution', ARRAY['equipement-et-alimentation--alimentation']),
        ('food_packages', ARRAY['equipement-et-alimentation--alimentation']),
        ('food', ARRAY['equipement-et-alimentation--alimentation']),
        ('french_course', ARRAY['apprendre-francais--suivre-formation']),
        ('health', ARRAY['sante']),
        ('legal_advice', ARRAY['acces-aux-droits-et-citoyennete--accompagnement-juridique']),
        ('long_term_accomodation', ARRAY['logement-hebergement--mal-loges-sans-logis']),
        ('mobility', ARRAY['mobilite']),
        ('other_activities', ARRAY['remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture']),
        ('overnight_stop', ARRAY['logement-hebergement--mal-loges-sans-logis']),
        ('parent_assistance', ARRAY['famille--information-et-accompagnement-des-parents','famille--soutien-a-la-parentalite', 'famille--soutien-aux-familles']),
        ('provision_of_vehicles', ARRAY['mobilite--louer-un-vehicule']),
        ('psychological_support', ARRAY['sante--bien-etre-psychologique']),
        ('public_writer', ARRAY['acces-aux-droits-et-citoyennete--accompagnement-dans-les-demarches-administratives', 'numerique--realiser-des-demarches-administratives-avec-un-accompagnement']),
        ('seated_catering', ARRAY['equipement-et-alimentation--alimentation']),
        ('social_grocery_stores', ARRAY['equipement-et-alimentation--alimentation']),
        ('solidarity_store', ARRAY['equipement-et-alimentation--habillement']),
        ('sport_activities', ARRAY['remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture']),
        ('std_testing', ARRAY['sante--prevention-et-acces-aux-soins']),
        ('telephone_at_your_disposal', ARRAY['equipement-et-alimentation--acces-a-un-telephone-et-un-abonnement']),
        ('wellness', ARRAY['remobilisation--bien-etre']),
        ('wifi', ARRAY['numerique--acceder-a-une-connexion-internet']),
    ) AS x (categorie, thematique)
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
    WHERE categorie IN (SELECT categorie FROM di_thematique_by_soliguide_categorie_code)
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
        (
            CURRENT_DATE AT TIME ZONE 'Europe/Paris',
            CURRENT_DATE AT TIME ZONE 'Europe/Paris'
        )
        OVERLAPS
        (
            COALESCE(close__date_debut, CURRENT_DATE - INTERVAL '1 year'),
            COALESCE(close__date_fin, CURRENT_DATE + INTERVAL '1 year')
        )
),


-- TODO(vmttn): clean up modes_orientation_* with dbt macros ?

final AS (
    SELECT
        open_services.id                                              AS "id",
        lieux.lieu_id                                                 AS "adresse_id",
        open_services._di_source_id                                   AS "source",
        NULL::TEXT []                                                 AS "types",
        NULL                                                          AS "prise_rdv",
        NULL::TEXT []                                                 AS "frais",
        NULL                                                          AS "frais_autres",
        NULL::TEXT []                                                 AS "profils",
        NULL::TEXT []                                                 AS "pre_requis",
        TRUE                                                          AS "cumulable",
        NULL::TEXT []                                                 AS "justificatifs",
        NULL::DATE                                                    AS "date_creation",
        NULL::DATE                                                    AS "date_suspension",
        filtered_phones.phone_number                                  AS "telephone",
        lieux.entity_mail                                             AS "courriel",
        NULL::BOOLEAN                                                 AS "contact_public",
        NULL                                                          AS "contact_nom_prenom",
        open_services.updated_at                                      AS "date_maj",
        'commune'                                                     AS "zone_diffusion_type",
        NULL                                                          AS "zone_diffusion_code",  -- will be overridden after geocoding
        NULL                                                          AS "zone_diffusion_nom",  -- will be overridden after geocoding
        NULL                                                          AS "formulaire_en_ligne",
        open_services.lieu_id                                         AS "structure_id",
        (
            SELECT di_thematique_by_soliguide_categorie_code.thematique
            FROM di_thematique_by_soliguide_categorie_code
            WHERE open_services.categorie = di_thematique_by_soliguide_categorie_code.categorie
        )::TEXT []                                                    AS "thematiques",
        ARRAY['en-presentiel']                                        AS "modes_accueil",
        categories.label || COALESCE(' : ' || open_services.name, '') AS "nom",
        'https://soliguide.fr/fr/fiche/' || lieux.seo_url             AS "lien_source",
        CASE LENGTH(open_services.description) <= 280
            WHEN TRUE THEN open_services.description
            WHEN FALSE THEN LEFT(open_services.description, 279) || '…'
        END                                                           AS "presentation_resume",
        CASE LENGTH(open_services.description) <= 280
            WHEN TRUE THEN NULL
            WHEN FALSE THEN open_services.description
        END                                                           AS "presentation_detail",
        CASE
            WHEN open_services.different_hours
                THEN UDF_SOLIGUIDE__NEW_HOURS_TO_OSM_OPENING_HOURS(open_services.hours)
            ELSE UDF_SOLIGUIDE__NEW_HOURS_TO_OSM_OPENING_HOURS(lieux.newhours)
        END                                                           AS "recurrence",
        ARRAY(
            SELECT DISTINCT m
            FROM
                UNNEST(
                    ARRAY_REMOVE(
                        CASE WHEN open_services.modalities__inconditionnel THEN ARRAY['telephoner'] END
                        || CASE WHEN open_services.modalities__appointment__checked THEN ARRAY['telephoner', 'envoyer-un-mail'] END
                        || CASE WHEN open_services.modalities__inscription__checked THEN ARRAY['telephoner', 'envoyer-un-mail'] END
                        || CASE WHEN open_services.modalities__orientation__checked THEN ARRAY['telephoner', 'envoyer-un-mail', 'envoyer-un-mail-avec-une-fiche-de-prescription'] END,
                        NULL
                    )
                ) AS m
        )                                                             AS "modes_orientation_accompagnateur",
        ARRAY_TO_STRING(
            ARRAY[
                CASE WHEN open_services.modalities__appointment__checked THEN '## Sur rendez-vous :' || E'\n' || open_services.modalities__appointment__precisions END,
                CASE WHEN open_services.modalities__inscription__checked THEN '## Sur inscription :' || E'\n' || open_services.modalities__inscription__precisions END,
                CASE WHEN open_services.modalities__orientation__checked THEN '## Sur orientation :' || E'\n' || open_services.modalities__orientation__precisions END
            ],
            E'\n\n'
        )                                                             AS "modes_orientation_accompagnateur_autres",
        ARRAY(
            SELECT DISTINCT m
            FROM
                UNNEST(
                    ARRAY_REMOVE(
                        CASE WHEN open_services.modalities__inconditionnel THEN ARRAY['se-presenter'] END
                        || CASE WHEN open_services.modalities__appointment__checked THEN ARRAY['telephoner', 'envoyer-un-mail'] END
                        || CASE WHEN open_services.modalities__inscription__checked THEN ARRAY['se-presenter', 'telephoner'] END
                        || CASE WHEN open_services.modalities__orientation__checked THEN ARRAY['autre'] END,
                        NULL
                    )
                ) AS m
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
    LEFT JOIN categories ON open_services.categorie = categories.code
    LEFT JOIN filtered_phones ON open_services.lieu_id = filtered_phones.lieu_id
    ORDER BY 1
)

SELECT * FROM final
