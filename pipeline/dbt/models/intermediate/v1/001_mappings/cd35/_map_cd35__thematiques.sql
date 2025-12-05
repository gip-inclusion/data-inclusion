{{ config(materialized='ephemeral') }}
SELECT x.*
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('accompagnement-educatif',                               'famille--soutien-a-la-parentalite-et-a-leducation'),
    ('accueil-collectif-permanent',                           'famille--garde-denfants'),
    ('accueil-collectif-ponctuel',                            'famille--garde-denfants'),
    ('accueil-de-nuit-des-sans-abris',                        'logement-hebergement--rechercher-une-solution-dhebergement-temporaire'),
    ('aide-a-la-famille',                                     'famille--soutien-a-la-parentalite-et-a-leducation'),
    ('bien-etre-mental',                                      'sante--sante-mentale'),
    ('ccas',                                                  NULL),
    ('colis-alimentaires',                                    'equipement-et-alimentation--alimentation'),
    ('consultation-juridique-gratuite',                       'difficultes-administratives-ou-juridiques--prendre-en-compte-une-problematique-judiciaire'),
    ('epiceries-sociales-et-solidaires',                      'equipement-et-alimentation--alimentation'),
    ('espaces-jeux',                                          NULL),
    ('esat',                                                  NULL),
    ('information-sur-la-formation-professionnelle-continue', 'se-former--monter-son-dossier-de-formation'),
    ('mairies',                                               NULL),
    ('maison-d-assistants-maternels',                         'famille--garde-denfants'),
    ('mission-locale',                                        NULL),
    ('services-administratifs-de-proximite',                  'difficultes-administratives-ou-juridiques--accompagnement-aux-demarches-administratives'),
    ('soins-infirmiers-a-domicile',                           'sante--acces-aux-soins')
    -- noqa: enable=layout.spacing
) AS x (cd35, data_inclusion)
WHERE x.data_inclusion IS NOT NULL
