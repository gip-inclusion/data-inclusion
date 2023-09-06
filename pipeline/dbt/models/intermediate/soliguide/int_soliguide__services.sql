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
        ('100', ARRAY['sante']),
        ('101', ARRAY['sante--faire-face-a-une-situation-daddiction']),
        ('102', ARRAY['sante--prevention-et-acces-aux-soins']),
        ('103', ARRAY['sante--bien-etre-psychologique']),
        ('201', ARRAY(SELECT value FROM thematiques WHERE value ~ '^numerique--')),
        ('202', ARRAY['apprendre-francais--suivre-formation']),
        ('303', ARRAY['remobilisation--bien-etre']),
        ('401', ARRAY['acces-aux-droits-et-citoyennete--accompagnement-juridique']),
        ('404', ARRAY[
            'acces-aux-droits-et-citoyennete--accompagnement-dans-les-demarches-administratives',
            'numerique--realiser-des-demarches-administratives-avec-un-accompagnement'
        ]),
        ('406', ARRAY[
            'acces-aux-droits-et-citoyennete--connaitre-ses-droits',
            'acces-aux-droits-et-citoyennete--accompagnement-dans-les-demarches-administratives'
        ]),
        ('407', ARRAY[
            'famille--information-et-accompagnement-des-parents',
            'famille--soutien-a-la-parentalite',
            'famille--soutien-aux-familles'
        ]),
        ('408', ARRAY(SELECT value FROM thematiques WHERE value ~ '^gestion-financiere--')),
        ('501', ARRAY['numerique--acceder-a-du-materiel']),
        ('502', ARRAY['numerique--acceder-a-une-connexion-internet']),
        ('504', ARRAY['equipement-et-alimentation--acces-a-un-telephone-et-un-abonnement']),
        ('600', ARRAY['equipement-et-alimentation--alimentation']),
        ('601', ARRAY['equipement-et-alimentation--alimentation']),
        ('602', ARRAY['equipement-et-alimentation--alimentation']),
        ('603', ARRAY['equipement-et-alimentation--alimentation']),
        ('604', ARRAY['equipement-et-alimentation--alimentation']),
        ('701', ARRAY['remobilisation--lien-social']),
        ('703', ARRAY['famille--garde-denfants']),
        ('704', ARRAY['famille--soutien-a-la-parentalite']),
        ('801', ARRAY['remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture']),
        ('804', ARRAY['remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture']),
        ('902', ARRAY['equipement-et-alimentation--habillement']),
        ('903', ARRAY['equipement-et-alimentation--habillement']),
        ('1200', ARRAY['mobilite']),
        ('1201', ARRAY['mobilite--comprendre-et-utiliser-les-transports-en-commun']),
        ('1202', ARRAY['mobilite--louer-un-vehicule']),
        ('1203', ARRAY['mobilite--comprendre-et-utiliser-les-transports-en-commun']),
        ('1300', ARRAY['logement-hebergement']),
        ('1301', ARRAY['logement-hebergement--mal-loges-sans-logis']),
        ('1302', ARRAY['logement-hebergement--mal-loges-sans-logis']),
        ('1303', ARRAY['logement-hebergement--mal-loges-sans-logis']),
        ('1305', ARRAY['logement-hebergement--etre-accompagne-pour-se-loger'])
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
        NULL                                                          AS "pre_requis",
        TRUE                                                          AS "cumulable",
        NULL                                                          AS "justificatifs",
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
        NULL::TEXT []                                                 AS "modes_orientation_accompagnateur",
        NULL::TEXT []                                                 AS "modes_orientation_beneficiaire",
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
        END                                                           AS "recurrence"
    FROM open_services
    LEFT JOIN lieux ON open_services.lieu_id = lieux.id
    LEFT JOIN categories ON open_services.categorie = categories.code
    LEFT JOIN filtered_phones ON open_services.lieu_id = filtered_phones.lieu_id
    ORDER BY 1
)

SELECT * FROM final
