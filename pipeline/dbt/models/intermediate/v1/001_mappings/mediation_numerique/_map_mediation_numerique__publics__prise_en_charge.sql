{{ config(materialized='ephemeral') }}

SELECT
    x.prise_en_charge,
    x.public
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('Déficience visuelle',          'personnes-en-situation-de-handicap'),
    ('Handicaps mentaux',            'personnes-en-situation-de-handicap'),
    ('Handicaps moteurs',            'personnes-en-situation-de-handicap'),
    ('Illettrisme',                  NULL),
    ('Langues étrangères (anglais)', 'personnes-exilees'),
    ('Langues étrangères (autres)',  'personnes-exilees'),
    ('Surdité',                      'personnes-en-situation-de-handicap')
    -- noqa: enable=layout.spacing
) AS x (prise_en_charge, public)
WHERE x.public IS NOT NULL
