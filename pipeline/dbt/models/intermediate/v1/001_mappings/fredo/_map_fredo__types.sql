{{ config(materialized='ephemeral') }}
SELECT x.*
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('accès aux droits',                                   'accompagnement'),
    ('accompagnement dans les démarches via le numérique', 'accompagnement'),
    ('accompagnement dans les démarches',                  'accompagnement'),
    ('accompagnement global',                              'accompagnement'),
    ('accompagnement psychologique',                       'accompagnement'),
    ('activités / ateliers',                               'atelier'),
    ('aide à la personne',                                 'accompagnement'),
    ('aide financière',                                    'aide-financiere'),
    ('aide logistique',                                    'aide-materielle'),
    ('apprentissage de la langue française',               'formation'),
    ('dispositif',                                         'accompagnement'),
    ('ecoute / soutien',                                   'accompagnement'),
    ('emploi',                                             'accompagnement'),
    ('immersion/ découverte',                              'information'),
    ('information / orientation',                          'information'),
    ('initiation',                                         'information'),
    ('soins',                                              'accompagnement')
    -- noqa: enable=layout.spacing
) AS x (service, type)
WHERE x.type IS NOT NULL
