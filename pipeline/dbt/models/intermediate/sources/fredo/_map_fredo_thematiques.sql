{{ config(materialized='ephemeral') }}
SELECT
    x.category,
    x.thematiques
FROM (
    VALUES
    ('accès à la propriété', 'logement-hebergement--etre-accompagne-dans-son-projet-accession'),
    ('accès au matériel', 'numerique--acceder-a-du-materiel'),
    ('accès au matériel informatique', 'numerique--s-equiper-en-materiel-informatique'),
    ('accès à un logement', 'logement-hebergement--etre-accompagne-pour-se-loger'),
    ('accès aux soins gratuits', 'sante--acces-aux-soins'),
    ('accès aux soins gratuits', 'sante--prevention-et-acces-aux-soins'),
    ('accès wifi', 'numerique--acceder-a-une-connexion-internet'),
    ('accompagnement dans les recherches', 'preparer-sa-candidature--organiser-ses-demarches-de-recherche-demploi'),
    ('accompagnement de personnes en insertion', 'accompagnement-social-et-professionnel-personnalise'),
    ('accompagnement de personnes en insertion', 'accompagnement-social-et-professionnel-personnalise--parcours-d-insertion-socioprofessionnel'),
    ('accompagnement social', 'accompagnement-social-et-professionnel-personnalise'),
    ('accompagnement social', 'accompagnement-social-et-professionnel-personnalise--parcours-d-insertion-socioprofessionnel'),
    ('addiction', 'sante--faire-face-a-une-situation-daddiction'),
    ('administratif', 'acces-aux-droits-et-citoyennete--accompagnement-dans-les-demarches-administratives'),
    ('aidants', 'handicap--aide-a-la-personne'),
    ('aide à la gestion du budget', 'gestion-financiere--apprendre-a-gerer-son-budget'),
    ('aide alimentaire', 'gestion-financiere--obtenir-une-aide-alimentaire'),
    ('aide alimentaire', 'equipement-et-alimentation--alimentation'),
    ('aide juridictionnelle', 'acces-aux-droits-et-citoyennete--accompagnement-juridique'),
    ('aide-ménagère', 'handicap--aide-a-la-personne'),
    ('alimentation / equipement', 'equipement-et-alimentation'),
    ('amélioration de l’habitat', 'logement-hebergement--besoin-dadapter-mon-logement'),
    ('ameublement', 'logement-hebergement--besoin-dadapter-mon-logement'),
    ('ameublement', 'equipement-et-alimentation--electromenager'),
    ('apprentissage', 'se-former--monter-son-dossier-de-formation'),
    ('apprentissage', 'se-former--trouver-sa-formation'),
    ('apprentissage lecture – écriture - calcul', 'illettrisme--info-acquisition-connaissances'),
    ('arts martiaux', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('assistant de service social', 'accompagnement-social-et-professionnel-personnalise--parcours-d-insertion-socioprofessionnel'),
    ('ateliers parents / enfants', 'famille--soutien-aux-familles'),
    ('avion', 'mobilite--etre-accompagne-dans-son-parcours-mobilite'),
    ('bénévolat', 'remobilisation--participer-a-des-actions-solidaires-ou-de-benevolat'),
    ('bus / vélo / trottinette', 'mobilite--comprendre-et-utiliser-les-transports-en-commun'),
    ('bus / vélo / trottinette', 'mobilite--apprendre-a-utiliser-un-deux-roues'),
    ('cantine', 'equipement-et-alimentation--alimentation'),
    ('citoyenneté', 'acces-aux-droits-et-citoyennete--accompagnement-dans-les-demarches-administratives'),
    ('citoyenneté', 'acces-aux-droits-et-citoyennete--faciliter-laction-citoyenne'),
    ('covoiturage', 'mobilite--etre-accompagne-dans-son-parcours-mobilite'),
    ('création d’entreprise', 'creation-activite--definir-son-projet-de-creation-dentreprise'),
    ('culture', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('danses', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('décès', 'sante--accompagner-les-traumatismes'),
    ('décès', 'famille--soutien-aux-familles'),
    ('déménagement', 'logement-hebergement--demenagement'),
    ('domiciliation', 'logement-hebergement--mal-loges-sans-logis'),
    ('electroménagers', 'equipement-et-alimentation--electromenager'),
    ('emploi', 'trouver-un-emploi'),
    ('emploi des femmes', 'trouver-un-emploi'),
    ('emploi / insertion / apprentissage', 'accompagnement-social-et-professionnel-personnalise--parcours-d-insertion-socioprofessionnel'),
    ('etablissements', 'se-former--trouver-sa-formation'),
    ('famille', 'famille'),
    ('fédérations', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('formation', 'se-former--monter-son-dossier-de-formation'),
    ('formations gratuites', 'se-former--trouver-sa-formation'),
    ('formations gratuites', 'se-former--monter-son-dossier-de-formation'),
    ('garde d’enfants', 'famille--garde-denfants'),
    ('gynécologie / contraception / ivg', 'sante--vie-relationnelle-et-affective'),
    ('gynécologie / contraception / ivg', 'sante--obtenir-la-prise-en-charge-de-frais-medicaux'),
    ('gynécologie / contraception / ivg', 'sante--accompagnement-de-la-femme-enceinte-du-bebe-et-du-jeune-enfant'),
    ('handicap', 'handicap'),
    ('handisport', 'handicap'),
    ('handisport', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('hébergement', 'logement-hebergement--etre-accompagne-pour-se-loger'),
    ('hébergement d’urgence', 'logement-hebergement--probleme-avec-son-logement'),
    ('hébergement d’urgence', 'logement-hebergement--mal-loges-sans-logis'),
    ('hébergement santé', 'logement-hebergement--probleme-avec-son-logement'),
    ('hébergement santé', 'logement-hebergement--mal-loges-sans-logis'),
    ('hébergement social', 'logement-hebergement--probleme-avec-son-logement'),
    ('hébergement social', 'logement-hebergement--mal-loges-sans-logis'),
    ('illettrisme/ apprentissage français / allophonie', 'apprendre-francais--communiquer-vie-tous-les-jours'),
    ('illettrisme/ apprentissage français / allophonie', 'illettrisme'),
    ('intérim', 'trouver-un-emploi--repondre-a-des-offres-demploi'),
    ('jouets', 'famille--accompagnement-femme-enceinte-bebe-jeune-enfant'),
    ('juridique', 'acces-aux-droits-et-citoyennete--accompagnement-juridique'),
    ('lieu ressource', 'accompagnement-social-et-professionnel-personnalise'),
    ('logement', 'logement-hebergement--etre-accompagne-pour-se-loger'),
    ('maintien dans le logement', 'logement-hebergement--probleme-avec-son-logement'),
    ('maladies chroniques', 'sante--acces-aux-soins'),
    ('maladies chroniques', 'sante--obtenir-la-prise-en-charge-de-frais-medicaux'),
    ('médiation – conciliation', 'acces-aux-droits-et-citoyennete--accompagnement-juridique'),
    ('mesure judiciaire', 'acces-aux-droits-et-citoyennete--accompagnement-juridique'),
    ('musée', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('musique', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('numérique', 'numerique'),
    ('nutrition', 'equipement-et-alimentation--alimentation'),
    ('objets', 'equipement-et-alimentation--aide-menagere'),
    ('parents / enfants', 'famille--soutien-aux-familles'),
    ('périscolaire', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('permis de conduire', 'mobilite--preparer-son-permis-de-conduire-se-reentrainer-a-la-conduite'),
    ('permis de conduire', 'mobilite--etre-accompagne-dans-son-parcours-mobilite'),
    ('personne de compagnie', 'remobilisation--lien-social'),
    ('personnes âgées', 'remobilisation--lien-social'),
    ('personnes dépendantes', 'remobilisation--lien-social'),
    ('personnes ressources dans les quartiers', 'remobilisation--lien-social'),
    ('petite enfance', 'famille--accompagnement-femme-enceinte-bebe-jeune-enfant'),
    ('petite enfance', 'famille--soutien-a-la-parentalite'),
    ('prévention', 'sante--prevention-et-acces-aux-soins'),
    ('protection / prévention / violences intrafamiliales', 'famille--violences-intrafamiliales'),
    ('protection / prévention / violences intrafamiliales', 'famille--jeunes-sans-soutien-familial'),
    ('recherche emploi', 'preparer-sa-candidature--organiser-ses-demarches-de-recherche-demploi'),
    ('reconnaissance du handicap', 'handicap--faire-reconnaitre-un-handicap'),
    ('remise en forme', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('réparation de matériel', 'numerique--acceder-a-du-materiel'),
    ('santé', 'sante--acces-aux-soins'),
    ('santé mentale', 'sante--bien-etre-psychologique'),
    ('scolarité', 'accompagnement-social-et-professionnel-personnalise--decrochage-scolaire'),
    ('scolarité', 'illettrisme--accompagner-scolarite'),
    ('scolarité', 'famille--information-et-accompagnement-des-parents'),
    ('soins à domicile', 'sante--acces-aux-soins'),
    ('sortir de l’isolement', 'remobilisation--lien-social'),
    ('soutien enfants déscolarisés / scolarité adaptée', 'accompagnement-social-et-professionnel-personnalise--decrochage-scolaire'),
    ('soutien scolaire', 'famille--information-et-accompagnement-des-parents'),
    ('soutien scolaire', 'illettrisme--accompagner-scolarite'),
    ('sport', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('sport de musculation', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('sport de réflexion', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('sports aquatiques', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('sports cyclistes', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('sports d''athlétisme', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('sports de ballon', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('sports de combat', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('sports de détente', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('sports de plein air', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('sports de raquette', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('sports équestres', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('sports gymniques', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('sports mécaniques', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('sports nautiques', 'remobilisation--decouvrir-son-potentiel-via-le-sport-et-la-culture'),
    ('stages', 'accompagnement-social-et-professionnel-personnalise--definition-du-projet-professionnel'),
    ('stages', 'choisir-un-metier--decouvrir-un-metier-ou-un-secteur-dactivite'),
    ('structures d’insertion par l’emploi', 'accompagnement-social-et-professionnel-personnalise--parcours-d-insertion-socioprofessionnel'),
    ('transport', 'mobilite--comprendre-et-utiliser-les-transports-en-commun'),
    ('transport', 'mobilite--etre-accompagne-dans-son-parcours-mobilite'),
    ('vêtements', 'equipement-et-alimentation--habillement'),
    ('vih', 'sante--vie-relationnelle-et-affective')
) AS x (category, thematiques)
