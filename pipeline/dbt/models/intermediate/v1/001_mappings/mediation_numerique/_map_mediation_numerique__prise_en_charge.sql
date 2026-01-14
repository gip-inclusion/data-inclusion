{{ config(materialized='ephemeral') }}

SELECT
    x.prise_en_charge_source,
    x.public AS "public"
FROM (
    VALUES
    ('Déficience visuelle', 'personnes-en-situation-de-handicap'),
    ('Handicaps mentaux', 'personnes-en-situation-de-handicap'),
    ('Handicaps moteurs', 'personnes-en-situation-de-handicap'),
    ('Illettrisme', NULL),
    ('Langues étrangères (anglais)', 'personnes-exilees'),
    ('Langues étrangères (autres)', 'personnes-exilees'),
    ('Surdité', 'personnes-en-situation-de-handicap')
) AS x (prise_en_charge_source, public)
