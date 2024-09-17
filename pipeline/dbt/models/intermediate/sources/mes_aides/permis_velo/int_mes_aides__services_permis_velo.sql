{% set check_structure_str = "Veuillez vérifier sur le site internet de la structure" %}

WITH permis_velo AS (
    SELECT * FROM {{ ref('stg_mes_aides__permis_velo') }}
),

communes AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__communes') }}
),

zone_code AS (
    SELECT
        permis_velo.id,
        communes.code,
        communes.siren_epci,
        communes.departement,
        communes.region
    FROM permis_velo
    LEFT JOIN communes ON permis_velo.code_postal = ANY(communes.codes_postaux) AND permis_velo.ville_nom = communes.nom
),

types_mapping AS (
    SELECT x.*
    FROM (
        VALUES
        ('Aide financière', 'aide-financiere'),
        ('Prestation', 'aide-materielle'),
        ('Accompagnement', 'accompagnement')
    ) AS x (types_mes_aides, types_di)
),

zone_de_diffusion_mapping AS (
    SELECT x.*
    FROM (
        VALUES
        ('Échelle communale', 'commune'),
        ('Échelle intercommunale', 'epci'),
        ('Échelle régionale', 'region'),
        ('Échelle départementale', 'departement'),
        ('France métropolitaine', 'pays')
    ) AS x (zone_mes_aides, zone_di)
),

zone_diffusion AS (
    SELECT
        zone_de_diffusion_mapping.zone_di,
        permis_velo.id
    FROM permis_velo
    INNER JOIN zone_de_diffusion_mapping ON permis_velo.zone_diffusion_type = zone_de_diffusion_mapping.zone_mes_aides
),

modes_orientation_mapping AS (
    SELECT x.*
    FROM (
        VALUES
        ('La démarche nécessite un passage ou une prise de contact directe avec votre organisme', ARRAY['envoyer-un-email', 'telephoner'], ARRAY['envoyer-un-email', 'telephoner', 'se-presenter']),
        ('La démarche nécessite une connexion sur le site de votre organisme', ARRAY['completer-le-formulaire-dadhesion'], ARRAY['completer-le-formulaire-dadhesion']),
        ('La démarche peut être faite par mail', ARRAY['envoyer-un-email'], ARRAY['envoyer-un-email'])
    ) AS x (demarche, modes_orientation_accompagnateur, modes_orientation_beneficiaire)
),

mapping_thematiques AS (
    SELECT x.*
    FROM (
        VALUES
        ('Financer mon permis', 'transport-et-mobilite', ARRAY['mobilite--financer-mon-projet-mobilite', 'mobilite--preparer-son-permis-de-conduire-se-reentrainer-a-la-conduite']),
        ('Financer mes trajets', 'transport-et-mobilite', ARRAY['mobilite--financer-mon-projet-mobilite']),
        ('Financer le carburant', 'transport-et-mobilite', ARRAY['mobilite--financer-mon-projet-mobilite']),
        ('Financer mon BSR', 'transport-et-mobilite', ARRAY['mobilite--financer-mon-projet-mobilite']),
        ('Financer l''achat d''un véhicule', 'transport-et-mobilite', ARRAY['mobilite--acheter-un-vehicule-motorise']),
        ('Déménager', 'logement', ARRAY['logement-hebergement--demenagement']),
        ('Transports en commun', 'transport-et-mobilite', ARRAY['mobilite--comprendre-et-utiliser-les-transports-en-commun']),
        ('Transport à la demande', 'transport-et-mobilite', ARRAY['mobilite--etre-accompagne-dans-son-parcours-mobilite']),
        ('Réparer ma voiture', 'transport-et-mobilite', ARRAY['mobilite--entretenir-reparer-son-vehicule']),
        ('Louer un vélo électrique', 'transport-et-mobilite', ARRAY['mobilite--louer-un-vehicule']),
        ('Ma voiture', 'transport-et-mobilite', ARRAY['mobilite--louer-un-vehicule', 'mobilite--entretenir-reparer-son-vehicule', 'mobilite--acheter-un-vehicule-motorise']),
        ('Mon vélo électrique', 'transport-et-mobilite', ARRAY['mobilite--acheter-un-velo', 'mobilite--entretenir-reparer-son-vehicule', 'mobilite--apprendre-a-utiliser-un-deux-roues', 'mobilite--louer-un-vehicule', 'mobilite--acheter-un-vehicule-motorise']),
        ('Mon scooter', 'transport-et-mobilite', ARRAY['mobilite--entretenir-reparer-son-vehicule', 'mobilite--apprendre-a-utiliser-un-deux-roues', 'mobilite--louer-un-vehicule']),
        ('Acheter un vélo électrique', 'transport-et-mobilite', ARRAY['mobilite--acheter-un-velo']),
        ('Acheter ou louer une voiture', 'transport-et-mobilite', ARRAY['mobilite--louer-un-vehicule', 'mobilite--acheter-un-vehicule-motorise']),
        ('Accéder au permis de conduire', 'transport-et-mobilite', ARRAY['mobilite--preparer-son-permis-de-conduire-se-reentrainer-a-la-conduite']),
        ('Déménager', 'transport-et-mobilite', ARRAY['logement-hebergement--demenagement']),
        ('Partir à l''étranger', 'transport-et-mobilite', ARRAY['souvrir-a-linternational--sinformer-sur-les-aides-pour-travailler-a-letranger']),
        ('Handicap & mobilité', 'transport-et-mobilite', ARRAY['handicap--favoriser-le-retour-et-le-maintien-dans-lemploi']),
        ('Trouver une alternance', 'trouver-un-job', ARRAY['trouver-un-emploi--suivre-ses-candidatures-et-relancer-les-employeurs', 'trouver-un-emploi--convaincre-un-recruteur-en-entretien', 'trouver-un-emploi--faire-des-candidatures-spontanees', 'trouver-un-emploi--repondre-a-des-offres-demploi']),
        ('Trouver un contrat d''apprentissage', 'trouver-un-job', ARRAY['trouver-un-emploi--suivre-ses-candidatures-et-relancer-les-employeurs', 'trouver-un-emploi--convaincre-un-recruteur-en-entretien', 'trouver-un-emploi--faire-des-candidatures-spontanees', 'trouver-un-emploi--repondre-a-des-offres-demploi']),
        ('Préparer une candidature', 'trouver-un-job', ARRAY['preparer-sa-candidature--valoriser-ses-competences']),
        ('Rédiger une lettre de motivation ou un CV', 'trouver-un-job', ARRAY['preparer-sa-candidature--realiser-un-cv-et-ou-une-lettre-de-motivation']),
        ('Devenir locataire', 'logement', ARRAY['logement-hebergement--etre-accompagne-pour-se-loger']),
        ('Financer mon loyer', 'logement', ARRAY['logement-hebergement--gerer-son-budget']),
        ('Faire des travaux', 'logement', ARRAY['logement-hebergement--besoin-dadapter-mon-logement']),
        ('Trouver un logement social', 'logement', ARRAY['logement-hebergement--etre-accompagne-pour-se-loger']),
        ('Financer une formation', 'formation-et-orientation', ARRAY['se-former--monter-son-dossier-de-formation']),
        ('Trouver une formation', 'formation-et-orientation', ARRAY['se-former--trouver-sa-formation']),
        ('Rémunération pendant la formation', 'formation-et-orientation', ARRAY['se-former--monter-son-dossier-de-formation']),
        ('Reconversion', 'formation-et-orientation', ARRAY['se-former--trouver-sa-formation']),
        ('Accompagnement personnalisé à la mobilité', 'transport-et-mobilite', ARRAY['mobilite--etre-accompagne-dans-son-parcours-mobilite'])
    ) AS x (besoins, thematiques, correspondance_di)
),

transformed_types AS (
    SELECT
        permis_velo.id,
        ARRAY_AGG(types_mapping.types_di) AS transformed_types
    FROM
        permis_velo,
        UNNEST(permis_velo.types) AS mes_aides_types
    LEFT JOIN
        types_mapping ON mes_aides_types = types_mapping.types_mes_aides
    GROUP BY
        permis_velo.id
),

final AS (
    SELECT
        permis_velo.id                                        AS "adresse_id",
        TRUE                                                  AS "contact_public",
        TRUE                                                  AS "cumulable",
        permis_velo.date_creation                             AS "date_creation",
        permis_velo.date_maj                                  AS "date_maj",
        CAST(NULL AS DATE)                                    AS "date_suspension",
        NULL                                                  AS "formulaire_en_ligne",
        permis_velo.frais_autres                              AS "frais_autres",
        permis_velo.id                                        AS "id",
        CAST(NULL AS TEXT [])                                 AS "justificatifs",
        permis_velo.lien_source                               AS "lien_source",
        CASE
            WHEN permis_velo.mode_acceuil = TRUE THEN ARRAY['a-distance']
            ELSE ARRAY['en-presentiel']
        END                                                   AS "modes_accueil",
        (
            SELECT modes_orientation_mapping.modes_orientation_accompagnateur
            FROM modes_orientation_mapping
            WHERE permis_velo.methode = modes_orientation_mapping.demarche
        )                                                     AS "modes_orientation_accompagnateur",
        NULL                                                  AS "modes_orientation_accompagnateur_autres",
        (
            SELECT modes_orientation_mapping.modes_orientation_beneficiaire
            FROM modes_orientation_mapping
            WHERE permis_velo.methode = modes_orientation_mapping.demarche
        )                                                     AS "modes_orientation_beneficiaire",
        permis_velo.mode_orientation_beneficiare_autre        AS "modes_orientation_beneficiaire_autres",
        permis_velo.nom                                       AS "nom",
        NULL                                                  AS "presentation_resume",
        permis_velo.presentation_detail                       AS "presentation_detail",
        NULL                                                  AS "prise_rdv",
        CAST(NULL AS TEXT [])                                 AS "profils",
        NULL                                                  AS "recurrence",
        permis_velo._di_source_id                             AS "source",
        COALESCE(permis_velo.siret_structure, permis_velo.id) AS "structure_id",
        (
            SELECT mapping_thematiques.correspondance_di
            FROM mapping_thematiques
            WHERE
                permis_velo.besoins_mes_aides = mapping_thematiques.besoins
                AND permis_velo.thematique_mes_aides = mapping_thematiques.thematiques
        )                                                     AS "thematiques",
        transformed_types.transformed_types                   AS "types",
        CASE
            WHEN zone_diffusion.zone_di = 'commune' THEN zone_code.code
            WHEN zone_diffusion.zone_di = 'epci' THEN zone_code.siren_epci
            WHEN zone_diffusion.zone_di = 'region' THEN zone_code.region
            WHEN zone_diffusion.zone_di = 'departement' THEN zone_code.departement
            WHEN zone_diffusion.zone_di = 'pays' THEN NULL
        END                                                   AS "zone_diffusion_code",
        CASE
            WHEN zone_diffusion.zone_di = 'commune' THEN permis_velo.ville_nom
            WHEN zone_diffusion.zone_di = 'epci' THEN permis_velo.ville_nom
            WHEN zone_diffusion.zone_di = 'region' THEN permis_velo.region
            WHEN zone_diffusion.zone_di = 'departement' THEN permis_velo.departement
            WHEN zone_diffusion.zone_di = 'pays' THEN NULL
        END                                                   AS "zone_diffusion_nom",
        zone_diffusion.zone_di                                AS "zone_diffusion_type",
        CAST(NULL AS TEXT [])                                 AS "pre_requis",
        NULL                                                  AS "contact_nom_prenom",
        permis_velo.courriel                                  AS "courriel",
        permis_velo.telephone                                 AS "telephone",
        CAST(NULL AS TEXT [])                                 AS "frais",
        permis_velo.page_web                                  AS "page_web"
    FROM permis_velo
    LEFT JOIN transformed_types ON permis_velo.id = transformed_types.id
    LEFT JOIN zone_diffusion ON permis_velo.id = zone_diffusion.id
    LEFT JOIN zone_code ON permis_velo.id = zone_code.id
)

SELECT * FROM final
