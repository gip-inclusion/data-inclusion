{{ config(materialized='ephemeral') }}
SELECT x.*
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('allocataires-rsa',                   'beneficiaires-des-minimas-sociaux'),
    ('associations',                       'actifs'),
    ('demandeurs-d-emploi',                'demandeurs-emploi'),
    ('enfants',                            'familles'),
    ('famille',                            'familles'),
    ('jeunes',                             'jeunes'),
    ('personnes-agees',                    'seniors'),
    ('personnes-en-situation-de-handicap', 'personnes-en-situation-de-handicap'),
    ('professionnels',                     'actifs'),
    ('retraites',                          'seniors')
    -- noqa: enable=layout.spacing
) AS x (profil, public)
WHERE x.public IS NOT NULL
