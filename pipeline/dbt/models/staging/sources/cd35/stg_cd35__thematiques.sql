SELECT x.*
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('Accompagnement éducatif',                                     'accompagnement-educatif'),
    ('Accueil collectif permanent : crèche - micro-crèche',         'accueil-collectif-permanent'),
    ('Accueil collectif ponctuel : halte garderie - multi accueil', 'accueil-collectif-ponctuel'),
    ('Accueil de nuit des sans abris',                              'accueil-de-nuit-des-sans-abris'),
    ('Aide à la famille',                                           'aide-a-la-famille'),
    ('Bien-être mental',                                            'bien-etre-mental'),
    ('Centre communal d''action sociale',                           'ccas'),
    ('Colis alimentaires',                                          'colis-alimentaires'),
    ('Consultation juridique gratuite',                             'consultation-juridique-gratuite'),
    ('Epiceries sociales et solidaires',                            'epiceries-sociales-et-solidaires'),
    ('Espaces jeux',                                                'espaces-jeux'),
    ('Etablissement et Service d''Aide par le Travail',             'esat'),
    ('Information sur la formation professionnelle continue',       'information-sur-la-formation-professionnelle-continue'),
    ('Mairies',                                                     'mairies'),
    ('Maison d''assistants maternels',                              'maison-d-assistants-maternels'),
    ('Mission locale',                                              'mission-locale'),
    ('Services administratifs de proximité - France services',      'services-administratifs-de-proximite'),
    ('Soins infirmiers à domicile',                                 'soins-infirmiers-a-domicile')
-- noqa: enable=layout.spacing
) AS x (raw, value)
