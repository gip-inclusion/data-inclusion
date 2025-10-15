WITH structures AS (
    SELECT * FROM {{ ref('stg_ma_boussole_aidants__structures') }}
),

services AS (
    SELECT * FROM {{ ref('stg_ma_boussole_aidants__structures__services') }}
),

solutions AS (
    SELECT * FROM {{ ref('stg_ma_boussole_aidants__solutions') }}
),

situations AS (
    SELECT * FROM {{ ref('stg_ma_boussole_aidants__structures__situations') }}
),

types_mapping AS (
    SELECT x.*
    FROM (
        VALUES
        (1, 'accompagnement'),  -- Premiers contacts
        (2, 'accompagnement'),  -- Démarches complémentaires
        (3, 'accompagnement'),  -- Appui Juridique
        (4, 'information'),  -- Comprendre la situation de mon proche
        (5, 'atelier'),  -- Soutien
        (6, 'information'),  -- Finances et patrimoine
        (7, 'aide-materielle'),  -- Aides à domicile
        (8, 'aide-materielle'),  -- Portage de repas
        (9, 'aide-materielle'),  -- Relais au domicile
        (10, 'accompagnement'),  -- Accompagnement à l’autonomie
        (11, 'aide-materielle'),  -- Soins à domicile
        (12, 'information'),  -- Points d’information
        (13, 'accompagnement'),  -- Diagnostic et adaptation du logement
        (14, 'aide-materielle'),  -- Equipement
        (15, 'information'),  -- Téléassistance
        (16, 'aide-materielle'),  -- Services de transports
        (17, 'accompagnement'),  -- Accompagnement aux déplacements
        (18, 'aide-materielle'),  -- Achat, location et adaptation de véhicule
        (19, 'formation'),  -- Formation à la conduite
        (20, 'accompagnement'),  -- Lieux de diagnostic
        (21, 'aide-materielle'),  -- Rééducation et suivi thérapeutique
        (23, 'accompagnement'),  -- Prévention santé
        (24, 'aide-materielle'),  -- Soins palliatifs
        (25, 'atelier'),  -- Soutien moral
        (26, 'formation'),  -- Formation de l’aidant
        (27, 'atelier'),  -- Bien-être
        (28, 'aide-materielle'),  -- Organisateurs de séjour
        (29, 'aide-materielle'),  -- Lieux de séjour / vacances
        (30, 'atelier'),  -- Convivialité
        (31, 'atelier'),  -- Activités physiques
        (32, 'atelier'),  -- Activités culturelles et artistiques
        (34, 'atelier'),  -- Activités de bien-être
        (35, 'aide-materielle'),  -- Etablissements pour personnes en situation de handicap
        (36, 'aide-materielle'),  -- Habitats alternatifs
        (37, 'aide-materielle'),  -- Résidences pour personnes âgées autonomes
        (38, 'aide-materielle'),  -- Résidences pour personnes âgées dépendantes
        (39, 'information'),  -- Points d’information sur le répit
        (40, 'aide-materielle'),  -- Relais au domicile
        (41, 'aide-materielle'),  -- Relais hors du domicile
        (42, 'aide-materielle'),  -- Crèches, assistantes maternelles
        (43, 'aide-materielle'),  -- Accueils de loisirs et péri-scolaires
        (44, 'aide-materielle'),  -- Baby-sitting
        (45, 'accompagnement'),  -- Scolarisation à l’école ordinaire
        (46, 'accompagnement'),  -- Scolarisation en établissements spécialisés
        (48, 'accompagnement'),  -- Aide aux étudiants
        (49, 'formation'),  -- Formations, insertion professionnelle
        (50, 'accompagnement'),  -- Secteur protégé ou adapté
        (51, 'aide-materielle')  -- Soins à domicile
    )
        AS x (code, label)
),

thematiques_mapping AS (
    SELECT x.*
    FROM (
        VALUES
        (1, ARRAY['difficultes-administratives-ou-juridiques--accompagnement-pour-lacces-aux-droits', 'difficultes-administratives-ou-juridiques--accompagnement-aux-demarches-administratives']),
        (2, ARRAY['difficultes-administratives-ou-juridiques--accompagnement-aux-demarches-administratives', 'difficultes-administratives-ou-juridiques--accompagnement-pour-lacces-aux-droits']),
        (3, ARRAY['difficultes-administratives-ou-juridiques--prendre-en-compte-une-problematique-judiciaire', 'difficultes-financieres--mettre-en-place-une-mesure-de-protection-financiere', 'difficultes-administratives-ou-juridiques--accompagnement-aux-demarches-administratives']),
        (4, ARRAY['sante--constituer-un-dossier-mdph-invalidite', 'famille--soutien-aidants']),
        (5, ARRAY['famille--soutien-aidants']),
        (6, ARRAY['famille--soutien-aidants']),
        (7, ARRAY['sante--acces-aux-soins', 'famille--soutien-aidants', 'equipement-et-alimentation--aide-menagere']),
        (8, ARRAY['equipement-et-alimentation--alimentation', 'equipement-et-alimentation--aide-menagere']),
        (9, ARRAY['famille--soutien-aidants', 'equipement-et-alimentation--aide-menagere']),
        (10, ARRAY['famille--soutien-aidants', 'difficultes-financieres--acquerir-une-autonomie-budgetaire']),
        (11, ARRAY['sante--acces-aux-soins', 'famille--soutien-aidants']),
        (12, ARRAY['logement-hebergement--sinformer-sur-les-demarches-liees-a-lacces-au-logement', 'famille--soutien-aidants']),
        (13, ARRAY['logement-hebergement--sinformer-sur-les-demarches-liees-a-lacces-au-logement', 'famille--soutien-aidants']),
        (14, ARRAY['equipement-et-alimentation--electromenager', 'numerique--acquerir-un-equipement', 'famille--soutien-aidants']),
        (15, ARRAY['sante--acces-aux-soins', 'famille--soutien-aidants']),
        (16, ARRAY['sante--acces-aux-soins']),
        (17, ARRAY['mobilite--etre-accompagne-dans-son-parcours-mobilite', 'mobilite--financer-ma-mobilite']),
        (18, ARRAY['mobilite--acceder-a-un-vehicule', 'mobilite--etre-accompagne-dans-son-parcours-mobilite']),
        (19, ARRAY['mobilite--preparer-un-permis']),
        (20, ARRAY['sante--acces-aux-soins', 'sante--sante-mentale']),
        (21, ARRAY['sante--acces-aux-soins']),
        (23, ARRAY['sante--sante-mentale', 'sante--acces-aux-soins']),
        (24, ARRAY['sante--sante-mentale', 'sante--acces-aux-soins']),
        (25, ARRAY['sante--sante-mentale']),
        (26, ARRAY['famille--soutien-aidants']),
        (27, ARRAY['remobilisation--activites-sportives-et-culturelles']),
        (28, ARRAY['remobilisation--activites-sportives-et-culturelles']),
        (29, ARRAY['remobilisation--activites-sportives-et-culturelles', 'famille--soutien-aidants']),
        (30, ARRAY['remobilisation--lien-social']),
        (31, ARRAY['remobilisation--activites-sportives-et-culturelles']),
        (32, ARRAY['remobilisation--activites-sportives-et-culturelles']),
        (34, ARRAY['remobilisation--bien-etre-confiance-en-soi', 'remobilisation--activites-sportives-et-culturelles']),
        (35, ARRAY['sante--acces-aux-soins', 'difficultes-administratives-ou-juridiques--accompagnement-pour-lacces-aux-droits']),
        (36, ARRAY['logement-hebergement--acheter-un-logement', 'logement-hebergement--louer-un-logement']),
        (37, ARRAY['sante--acces-aux-soins']),
        (38, ARRAY['sante--acces-aux-soins']),
        (39, ARRAY['famille--soutien-aidants']),
        (40, ARRAY['famille--soutien-aidants', 'equipement-et-alimentation--aide-menagere']),
        (41, ARRAY['famille--soutien-aidants', 'equipement-et-alimentation--aide-menagere']),
        (42, ARRAY['famille--garde-denfants']),
        (43, ARRAY['famille--garde-denfants', 'remobilisation--activites-sportives-et-culturelles']),
        (44, ARRAY['famille--garde-denfants']),
        (45, ARRAY['famille--garde-denfants']),
        (46, ARRAY['famille--garde-denfants', 'sante--acces-aux-soins']),
        (48, ARRAY['difficultes-administratives-ou-juridiques--accompagnement-pour-lacces-aux-droits', 'logement-hebergement--rechercher-une-solution-dhebergement-temporaire', 'equipement-et-alimentation--alimentation']),
        (49, ARRAY['se-former--monter-son-dossier-de-formation', 'trouver-un-emploi--maintien-dans-lemploi']),
        (50, ARRAY['trouver-un-emploi--maintien-dans-lemploi']),
        (51, ARRAY['sante--acces-aux-soins'])
    )
        AS x (code, thematiques)
),

profils_mapping AS (
    SELECT * FROM (
        VALUES
        ('1', 'jeunes'),
        ('2', 'demandeurs-demploi'),
        ('2', 'salaries'),
        ('3', 'seniors-65')
    ) AS x (id_profil, profil_value)
),

profils AS (
    SELECT
        situations.id_structure,
        ARRAY_AGG(
            DISTINCT profils_mapping.profil_value
            ORDER BY profils_mapping.profil_value
        ) AS profils
    FROM situations
    INNER JOIN profils_mapping ON situations.id_profil = profils_mapping.id_profil
    GROUP BY situations.id_structure
),

final AS (
    SELECT
        'ma-boussole-aidants'                                                 AS "source",
        services.id_structure                                                 AS "structure_id",
        services.id_structure                                                 AS "adresse_id",
        services.id_structure || '-' || services.id_sous_thematique_solutions AS "id",
        solutions.label                                                       AS "nom",
        NULL                                                                  AS "presentation_resume",
        solutions.description                                                 AS "presentation_detail",
        NULL                                                                  AS "date_creation",
        structures.last_modified_date                                         AS "date_maj",
        ARRAY[types_mapping.label]                                            AS "types",
        thematiques_mapping.thematiques                                       AS "thematiques",
        ARRAY['gratuit']                                                      AS "frais",
        NULL                                                                  AS "frais_autres",
        NULL                                                                  AS "prise_rdv",
        NULL                                                                  AS "conditions_acces",
        structures.telephone_1                                                AS "telephone",
        structures.email                                                      AS "courriel",
        CASE services."is_distanciel"
            WHEN TRUE THEN ARRAY['en-presentiel', 'a-distance']
            ELSE ARRAY['en-presentiel']
        END                                                                   AS "modes_accueil",
        NULL                                                                  AS "contact_nom_prenom",
        NULL                                                                  AS "lien_mobilisation",
        profils.profils                                                       AS "profils",
        ARRAY_TO_STRING(
            ARRAY[
                'Âge minimum : ' || structures.age_min,
                'Âge maximum : ' || structures.age_max
            ],
            ' '
        )                                                                     AS "profils_precisions",
        CASE structures.rayon_action__nom_rayon_action
            WHEN 'communal' THEN 'commune'
            WHEN 'départemental' THEN 'departement'
            WHEN 'local' THEN 'region'
            ELSE 'pays'
        END                                                                   AS "zone_diffusion_type",
        structures.departement__code_departement                              AS "zone_diffusion_code",
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN structures.telephone_1 IS NOT NULL THEN 'telephoner' END,
                CASE WHEN structures.email IS NOT NULL THEN 'envoyer-un-mail' END
            ],
            NULL
        )                                                                     AS "modes_orientation_accompagnateur",
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN structures.telephone_1 IS NOT NULL THEN 'telephoner' END,
                CASE WHEN structures.email IS NOT NULL THEN 'envoyer-un-mail' END,
                CASE WHEN structures.telephone_1 IS NULL AND structures.email IS NULL THEN 'se-presenter' END
            ],
            NULL
        )                                                                     AS "modes_orientation_beneficiaire"
    FROM services
    LEFT JOIN structures ON services.id_structure = structures.id_structure
    LEFT JOIN solutions ON services.id_sous_thematique_solutions = solutions.code
    LEFT JOIN thematiques_mapping ON services.id_sous_thematique_solutions = thematiques_mapping.code
    LEFT JOIN types_mapping ON services.id_sous_thematique_solutions = types_mapping.code
    LEFT JOIN profils ON services.id_structure = profils.id_structure
)

SELECT * FROM final
