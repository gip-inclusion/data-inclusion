SELECT x.*
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('Allocataires RSA',                   'allocataires-rsa'),
    ('associations',                       'associations'),
    ('demandeurs d''emploi',               'demandeurs-d-emploi'),
    ('enfants',                            'enfants'),
    ('Famille',                            'famille'),
    ('Jeunes',                             'jeunes'),
    ('Personnes âgées',                    'personnes-agees'),
    ('Personnes en situation de handicap', 'personnes-en-situation-de-handicap'),
    ('Professionnels',                     'professionnels'),
    ('retraités',                          'retraites')
-- noqa: enable=layout.spacing
) AS x (raw, value)
