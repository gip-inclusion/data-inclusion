{{ config(materialized='ephemeral') }}
SELECT x.*
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('adulte',                               NULL),
    ('détenu ou sortant de détention',       'personnes-en-situation-juridique-specifique'),
    ('enfant',                               'jeunes'),
    ('etudiant',                             'etudiants'),
    ('femme',                                'femmes'),
    ('jeune',                                'jeunes'),
    ('personne âgée',                        'seniors'),
    ('personne de nationalité étrangère',    'personnes-exilees'),
    ('personne dépendante',                  'personnes-en-situation-de-handicap'),
    ('porteur de handicap',                  'personnes-en-situation-de-handicap'),
    ('salarié',                              'actifs'),
    ('sans domicile fixe',                   'personnes-en-situation-durgence'),
    ('sortant ase',                          'jeunes'),
    ('tout public',                          'tous-publics'),
    ('travailleur en situation de handicap', 'actifs'),
    ('travailleur en situation de handicap', 'personnes-en-situation-de-handicap'),
    ('victime',                              'personnes-en-situation-durgence')
    -- noqa: enable=layout.spacing
) AS x (public_fredo, public_di)
WHERE x.public_di IS NOT NULL
