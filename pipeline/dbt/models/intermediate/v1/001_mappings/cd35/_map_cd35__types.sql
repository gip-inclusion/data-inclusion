{{ config(materialized='ephemeral') }}
SELECT x.*
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('accompagnement-educatif',                               'accompagnement'),
    ('accueil-collectif-permanent',                           'aide-materielle'),
    ('accueil-collectif-ponctuel',                            'aide-materielle'),
    ('accueil-de-nuit-des-sans-abris',                        'aide-materielle'),
    ('aide-a-la-famille',                                     'accompagnement'),
    ('bien-etre-mental',                                      'accompagnement'),
    ('ccas',                                                  NULL),
    ('colis-alimentaires',                                    'aide-materielle'),
    ('consultation-juridique-gratuite',                       'accompagnement'),
    ('epiceries-sociales-et-solidaires',                      'aide-materielle'),
    ('espaces-jeux',                                          NULL),
    ('esat',                                                  NULL),
    ('information-sur-la-formation-professionnelle-continue', 'information'),
    ('mairies',                                               NULL),
    ('maison-d-assistants-maternels',                         'accompagnement'),
    ('mission-locale',                                        NULL),
    ('services-administratifs-de-proximite',                  'accompagnement'),
    ('soins-infirmiers-a-domicile',                           'accompagnement')
    -- noqa: enable=layout.spacing
) AS x (thematique, type)
WHERE x.type IS NOT NULL
