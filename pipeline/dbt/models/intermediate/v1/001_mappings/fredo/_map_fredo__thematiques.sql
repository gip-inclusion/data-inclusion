{{ config(materialized='ephemeral') }}
-- These are the most frequent categories at time of writing.
-- Given the large number of categories and for maintainability reasons,
-- we only handle the most frequent categories and leave it to fredo
-- to better categorize.
SELECT x.*
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('accès à un logement',                                   'logement-hebergement--sinformer-sur-les-demarches-liees-a-lacces-au-logement'),
    ('accès wifi',                                            'numerique--acceder-a-une-connexion-internet'),
    ('accompagnement dans les recherches',                    'preparer-sa-candidature--organiser-ses-demarches-de-recherche-demploi'),
    ('accompagnement de personnes en insertion',              'preparer-sa-candidature--organiser-ses-demarches-de-recherche-demploi'),
    ('accompagnement de personnes en insertion',              'trouver-un-emploi--maintien-dans-lemploi'),
    ('accompagnement social',                                 NULL),
    ('administratif',                                         'difficultes-administratives-ou-juridiques--accompagnement-aux-demarches-administratives'),
    ('aidants',                                               'famille--soutien-aidants'),
    ('aide-ménagère',                                         'equipement-et-alimentation--aide-menagere'),
    ('alimentation / equipement',                             'equipement-et-alimentation--aide-menagere'),
    ('alimentation / equipement',                             'equipement-et-alimentation--alimentation'),
    ('alimentation / equipement',                             'equipement-et-alimentation--electromenager'),
    ('alimentation / equipement',                             'equipement-et-alimentation--habillement'),
    ('apprentissage',                                         'se-former--monter-son-dossier-de-formation'),
    ('création d’entreprise',                                 'creer-une-entreprise--definir-son-projet-de-creation-dentreprise'),
    ('culture',                                               'remobilisation--activites-sportives-et-culturelles'),
    ('emploi / insertion / apprentissage',                    'preparer-sa-candidature--organiser-ses-demarches-de-recherche-demploi'),
    ('emploi / insertion / apprentissage',                    'trouver-un-emploi--maintien-dans-lemploi'),
    ('etablissements',                                        NULL),
    ('famille',                                               'famille--soutien-a-la-parentalite-et-a-leducation'),
    ('formation',                                             'se-former--monter-son-dossier-de-formation'),
    ('garde d’enfants',                                       'famille--garde-denfants'),
    ('juridique',                                             'difficultes-administratives-ou-juridiques--prendre-en-compte-une-problematique-judiciaire'),
    ('lieu ressource',                                        'choisir-un-metier--confirmer-son-choix-de-metier'),
    ('lieu ressource',                                        'famille--soutien-a-la-parentalite-et-a-leducation'),
    ('logement',                                              'logement-hebergement--se-maintenir-dans-le-logement'),
    ('numérique',                                             'numerique--acceder-a-des-services-en-ligne'),
    ('numérique',                                             'numerique--acceder-a-une-connexion-internet'),
    ('numérique',                                             'numerique--acquerir-un-equipement'),
    ('numérique',                                             'numerique--maitriser-les-fondamentaux-du-numerique'),
    ('parents / enfants',                                     'famille--soutien-a-la-parentalite-et-a-leducation'),
    ('personnes âgées',                                       NULL),
    ('prévention',                                            'sante--acces-aux-soins'),
    ('protection / prévention / violences intrafamiliales',   'famille--surmonter-conflits-separation-violence'),
    ('recherche emploi',                                      'preparer-sa-candidature--organiser-ses-demarches-de-recherche-demploi'),
    ('santé mentale',                                         'sante--sante-mentale'),
    ('santé',                                                 'sante--acces-aux-soins'),
    ('scolarité',                                             'famille--soutien-a-la-parentalite-et-a-leducation'),
    ('sport',                                                 'remobilisation--activites-sportives-et-culturelles'),
    ('sports de ballon',                                      'remobilisation--activites-sportives-et-culturelles'),
    ('transport',                                             'mobilite--etre-accompagne-dans-son-parcours-mobilite')
    -- noqa: enable=layout.spacing
) AS x (category, thematique)
WHERE x.thematique IS NOT NULL
