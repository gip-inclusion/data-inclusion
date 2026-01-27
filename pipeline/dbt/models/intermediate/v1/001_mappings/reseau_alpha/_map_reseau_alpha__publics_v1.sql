{{ config(materialized='ephemeral') }}
SELECT
    x.public_reseau_alpha,
    x.public_datainclusion
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('beneficiaires-de-la-protection-temporaire',              'personnes-exilees'),
    ('beneficiaires-du-rsa',                                   'beneficiaires-des-minimas-sociaux'),
    ('demandeurs-d-asile',                                     'personnes-exilees'),
    ('famille-monoparentale',                                  'familles'),
    ('femmes',                                                 'femmes'),
    ('habitant-quartier-politique-de-la-ville',                'residents-qpv-frr'),
    ('habitant-un-territoire-precis',                          NULL),
    ('jeune-16-25-ans',                                        'jeunes'),
    ('minorites-de-genre',                                     NULL),
    ('oriente-par-des-prescripteurs',                          NULL),
    ('parents',                                                'familles'),
    ('parents-d-eleves',                                       'familles'),
    ('personnes-agees-plus-de-60-ans',                         'seniors'),
    ('primo-arrivants-signataires-du-cir',                     'personnes-exilees'),
    ('refugies-beneficiaires-de-la-protection-internationale', 'personnes-exilees'),
    ('residents-de-centre-d-hebergement-ou-de-cada',           NULL),
    ('tout-public',                                            'tous-publics'),
    ('publics-en-situation-de-handicap',                       'personnes-en-situation-de-handicap')
    -- noqa: enable=layout.spacing
) AS x (public_reseau_alpha, public_datainclusion)
WHERE x.public_datainclusion IS NOT NULL
