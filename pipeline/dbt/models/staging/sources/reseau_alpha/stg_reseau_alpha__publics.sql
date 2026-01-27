SELECT x.*
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('Bénéficiaires de la protection temporaire (déplacés ukrainiens)', 'beneficiaires-de-la-protection-temporaire'),
    ('Bénéficiaires du RSA',                                            'beneficiaires-du-rsa'),
    ('Demandeurs d''asile',                                             'demandeurs-d-asile'),
    ('Famille monoparentale',                                           'famille-monoparentale'),
    ('Femmes',                                                          'femmes'),
    ('Habitant Quartier Politique de la Ville',                         'habitant-quartier-politique-de-la-ville'),
    ('Habitant un territoire précis',                                   'habitant-un-territoire-precis'),
    ('Jeune (16-25 ans)',                                               'jeune-16-25-ans'),
    ('Minorités de genre',                                              'minorites-de-genre'),
    ('Orienté par des prescripteurs',                                   'oriente-par-des-prescripteurs'),
    ('Parents',                                                         'parents'),
    ('Parents d''élèves',                                               'parents-d-eleves'),
    ('Personnes âgées (+60 ans)',                                       'personnes-agees-plus-de-60-ans'),
    ('Primo-arrivants signataires du CIR',                              'primo-arrivants-signataires-du-cir'),
    ('Réfugiés / Bénéficiaires de la protection internationale',        'refugies-beneficiaires-de-la-protection-internationale'),
    ('Résidents de centre d''hébergement ou de CADA',                   'residents-de-centre-d-hebergement-ou-de-cada'),
    ('Tout public',                                                     'tout-public'),
    ('Publics en situation de handicap',                                'publics-en-situation-de-handicap')
    -- noqa: enable=layout.spacing
) AS x (raw, value)
