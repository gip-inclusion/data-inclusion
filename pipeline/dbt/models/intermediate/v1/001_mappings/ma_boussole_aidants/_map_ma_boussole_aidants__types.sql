{{ config(materialized='ephemeral') }}
SELECT x.*
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    (1,  'accompagnement'),  -- Premiers contacts
    (2,  'accompagnement'),  -- Démarches complémentaires
    (3,  'accompagnement'),  -- Appui Juridique
    (4,  'information'),     -- Comprendre la situation de mon proche
    (5,  'atelier'),         -- Soutien
    (6,  'information'),     -- Finances et patrimoine
    (7,  'aide-materielle'), -- Aides à domicile
    (8,  'aide-materielle'), -- Portage de repas
    (9,  'aide-materielle'), -- Relais au domicile
    (10, 'accompagnement'),  -- Accompagnement à l’autonomie
    (11, 'aide-materielle'), -- Soins à domicile
    (12, 'information'),     -- Points d’information
    (13, 'accompagnement'),  -- Diagnostic et adaptation du logement
    (14, 'aide-materielle'), -- Equipement
    (15, 'information'),     -- Téléassistance
    (16, 'aide-materielle'), -- Services de transports
    (17, 'accompagnement'),  -- Accompagnement aux déplacements
    (18, 'aide-materielle'), -- Achat, location et adaptation de véhicule
    (19, 'formation'),       -- Formation à la conduite
    (20, 'accompagnement'),  -- Lieux de diagnostic
    (21, 'aide-materielle'), -- Rééducation et suivi thérapeutique
    (23, 'accompagnement'),  -- Prévention santé
    (24, 'aide-materielle'), -- Soins palliatifs
    (25, 'atelier'),         -- Soutien moral
    (26, 'formation'),       -- Formation de l’aidant
    (27, 'atelier'),         -- Bien-être
    (28, 'aide-materielle'), -- Organisateurs de séjour
    (29, 'aide-materielle'), -- Lieux de séjour / vacances
    (30, 'atelier'),         -- Convivialité
    (31, 'atelier'),         -- Activités physiques
    (32, 'atelier'),         -- Activités culturelles et artistiques
    (34, 'atelier'),         -- Activités de bien-être
    (35, 'aide-materielle'), -- Etablissements pour personnes en situation de handicap
    (36, 'aide-materielle'), -- Habitats alternatifs
    (37, 'aide-materielle'), -- Résidences pour personnes âgées autonomes
    (38, 'aide-materielle'), -- Résidences pour personnes âgées dépendantes
    (39, 'information'),     -- Points d’information sur le répit
    (40, 'aide-materielle'), -- Relais au domicile
    (41, 'aide-materielle'), -- Relais hors du domicile
    (42, 'aide-materielle'), -- Crèches, assistantes maternelles
    (43, 'aide-materielle'), -- Accueils de loisirs et péri-scolaires
    (44, 'aide-materielle'), -- Baby-sitting
    (45, 'accompagnement'),  -- Scolarisation à l’école ordinaire
    (46, 'accompagnement'),  -- Scolarisation en établissements spécialisés
    (48, 'accompagnement'),  -- Aide aux étudiants
    (49, 'formation'),       -- Formations, insertion professionnelle
    (50, 'accompagnement'),  -- Secteur protégé ou adapté
    (51, 'aide-materielle')  -- Soins à domicile
    -- noqa: enable=layout.spacing
) AS x (solution_id, "type")
