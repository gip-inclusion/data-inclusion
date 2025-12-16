{{ config(materialized='ephemeral') }}
SELECT
    x.solution_id,
    UNNEST(x.thematiques) AS "thematique"
FROM (
    VALUES
    (
        1,  -- Premiers contacts
        ARRAY[
            'difficultes-administratives-ou-juridiques--accompagnement-pour-lacces-aux-droits',
            'difficultes-administratives-ou-juridiques--accompagnement-aux-demarches-administratives'
        ]
    ),
    (
        2,  -- Démarches complémentaires
        ARRAY[
            'difficultes-administratives-ou-juridiques--accompagnement-aux-demarches-administratives',
            'difficultes-administratives-ou-juridiques--accompagnement-pour-lacces-aux-droits'
        ]
    ),
    (
        3,  -- Appui Juridique
        ARRAY[
            'difficultes-administratives-ou-juridiques--prendre-en-compte-une-problematique-judiciaire',
            'difficultes-financieres--mettre-en-place-une-mesure-de-protection-financiere',
            'difficultes-administratives-ou-juridiques--accompagnement-aux-demarches-administratives'
        ]
    ),
    (
        4,  -- Comprendre la situation de mon proche
        ARRAY[
            'sante--constituer-un-dossier-mdph-invalidite',
            'famille--soutien-aidants'
        ]
    ),
    (
        5,  -- Soutien
        ARRAY[
            'famille--soutien-aidants'
        ]
    ),
    (
        6,  -- Finances et patrimoine
        ARRAY[
            'famille--soutien-aidants'
        ]
    ),
    (
        7,  -- Aides à domicile
        ARRAY[
            'sante--acces-aux-soins',
            'famille--soutien-aidants',
            'equipement-et-alimentation--aide-menagere'
        ]
    ),
    (
        8,  -- Portage de repas
        ARRAY[
            'equipement-et-alimentation--alimentation',
            'equipement-et-alimentation--aide-menagere'
        ]
    ),
    (
        9,  -- Relais au domicile
        ARRAY[
            'famille--soutien-aidants',
            'equipement-et-alimentation--aide-menagere'
        ]
    ),
    (
        10,  -- Accompagnement à l’autonomie
        ARRAY[
            'famille--soutien-aidants',
            'difficultes-financieres--acquerir-une-autonomie-budgetaire'
        ]
    ),
    (
        11,  -- Soins à domicile
        ARRAY[
            'sante--acces-aux-soins',
            'famille--soutien-aidants'
        ]
    ),
    (
        12,  -- Points d’information
        ARRAY[
            'logement-hebergement--sinformer-sur-les-demarches-liees-a-lacces-au-logement',
            'famille--soutien-aidants'
        ]
    ),
    (
        13,  -- Diagnostic et adaptation du logement
        ARRAY[
            'logement-hebergement--sinformer-sur-les-demarches-liees-a-lacces-au-logement',
            'famille--soutien-aidants'
        ]
    ),
    (
        14,  -- Equipement
        ARRAY[
            'equipement-et-alimentation--electromenager',
            'numerique--acquerir-un-equipement',
            'famille--soutien-aidants'
        ]
    ),
    (
        15,  -- Téléassistance
        ARRAY[
            'sante--acces-aux-soins',
            'famille--soutien-aidants'
        ]
    ),
    (
        16,  -- Services de transports
        ARRAY[
            'sante--acces-aux-soins'
        ]
    ),
    (
        17,  -- Accompagnement aux déplacements
        ARRAY[
            'mobilite--etre-accompagne-dans-son-parcours-mobilite',
            'mobilite--financer-ma-mobilite'
        ]
    ),
    (
        18,  -- Achat, location et adaptation de véhicule
        ARRAY[
            'mobilite--acceder-a-un-vehicule',
            'mobilite--etre-accompagne-dans-son-parcours-mobilite'
        ]
    ),
    (
        19,  -- Formation à la conduite
        ARRAY[
            'mobilite--preparer-un-permis'
        ]
    ),
    (
        20,  -- Lieux de diagnostic
        ARRAY[
            'sante--acces-aux-soins',
            'sante--sante-mentale'
        ]
    ),
    (
        21,  -- Rééducation et suivi thérapeutique
        ARRAY[
            'sante--acces-aux-soins'
        ]
    ),
    (
        23,  -- Prévention santé
        ARRAY[
            'sante--sante-mentale',
            'sante--acces-aux-soins'
        ]
    ),
    (
        24,  -- Soins palliatifs
        ARRAY[
            'sante--sante-mentale',
            'sante--acces-aux-soins'
        ]
    ),
    (
        25,  -- Soutien moral
        ARRAY[
            'sante--sante-mentale'
        ]
    ),
    (
        26,  -- Formation de l’aidant
        ARRAY[
            'famille--soutien-aidants'
        ]
    ),
    (
        27,  -- Bien-être
        ARRAY[
            'remobilisation--activites-sportives-et-culturelles'
        ]
    ),
    (
        28,  -- Organisateurs de séjour
        ARRAY[
            'remobilisation--activites-sportives-et-culturelles'
        ]
    ),
    (
        29,  -- Lieux de séjour / vacances
        ARRAY[
            'remobilisation--activites-sportives-et-culturelles',
            'famille--soutien-aidants'
        ]
    ),
    (
        30,  -- Convivialité
        ARRAY[
            'remobilisation--lien-social'
        ]
    ),
    (
        31,  -- Activités physiques
        ARRAY[
            'remobilisation--activites-sportives-et-culturelles'
        ]
    ),
    (
        32,  -- Activités culturelles et artistiques
        ARRAY[
            'remobilisation--activites-sportives-et-culturelles'
        ]
    ),
    (
        34,  -- Activités de bien-être
        ARRAY[
            'remobilisation--bien-etre-confiance-en-soi',
            'remobilisation--activites-sportives-et-culturelles'
        ]
    ),
    (
        35,  -- Etablissements pour personnes en situation de handicap
        ARRAY[
            'sante--acces-aux-soins',
            'difficultes-administratives-ou-juridiques--accompagnement-pour-lacces-aux-droits'
        ]
    ),
    (
        36,  -- Habitats alternatifs
        ARRAY[
            'logement-hebergement--acheter-un-logement',
            'logement-hebergement--louer-un-logement'
        ]
    ),
    (
        37,  -- Résidences pour personnes âgées autonomes
        ARRAY[
            'sante--acces-aux-soins'
        ]
    ),
    (
        38,  -- Résidences pour personnes âgées dépendantes
        ARRAY[
            'sante--acces-aux-soins'
        ]
    ),
    (
        39,  -- Points d’information sur le répit
        ARRAY[
            'famille--soutien-aidants'
        ]
    ),
    (
        40,  -- Relais au domicile
        ARRAY[
            'famille--soutien-aidants',
            'equipement-et-alimentation--aide-menagere'
        ]
    ),
    (
        41,  -- Relais hors du domicile
        ARRAY[
            'famille--soutien-aidants',
            'equipement-et-alimentation--aide-menagere'
        ]
    ),
    (
        42,  -- Crèches, assistantes maternelles
        ARRAY[
            'famille--garde-denfants'
        ]
    ),
    (
        43,  -- Accueils de loisirs et péri-scolaires
        ARRAY[
            'famille--garde-denfants',
            'remobilisation--activites-sportives-et-culturelles'
        ]
    ),
    (
        44,  -- Baby-sitting
        ARRAY[
            'famille--garde-denfants'
        ]
    ),
    (
        45,  -- Scolarisation à l’école ordinaire
        ARRAY[
            'famille--garde-denfants'
        ]
    ),
    (
        46,  -- Scolarisation en établissements spécialisés
        ARRAY[
            'famille--garde-denfants',
            'sante--acces-aux-soins'
        ]
    ),
    (
        48,  -- Aide aux étudiants
        ARRAY[
            'difficultes-administratives-ou-juridiques--accompagnement-pour-lacces-aux-droits',
            'logement-hebergement--rechercher-une-solution-dhebergement-temporaire',
            'equipement-et-alimentation--alimentation'
        ]
    ),
    (
        49,  -- Formations, insertion professionnelle
        ARRAY[
            'se-former--monter-son-dossier-de-formation',
            'trouver-un-emploi--maintien-dans-lemploi'
        ]
    ),
    (
        50,  -- Secteur protégé ou adapté
        ARRAY[
            'trouver-un-emploi--maintien-dans-lemploi'
        ]
    ),
    (
        51,  -- Soins à domicile
        ARRAY[
            'sante--acces-aux-soins'
        ]
    )
) AS x (solution_id, thematiques)
