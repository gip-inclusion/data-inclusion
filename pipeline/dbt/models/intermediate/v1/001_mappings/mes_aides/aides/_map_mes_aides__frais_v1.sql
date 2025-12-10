{{ config(materialized='ephemeral') }}
SELECT x.*
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('facilitation de paiement',            'payant'),
    ('pret remboursement',                  'gratuit'),
    ('prise en charge partielle ou totale', 'gratuit'),
    ('prise en charge partielle',           'payant'),
    ('prise en charge totale',              'gratuit'),
    ('somme d''argent',                     'payant'),
    ('tarif preferentiel',                  'payant')
    -- noqa: enable=layout.spacing
) AS x (nature, frais)
