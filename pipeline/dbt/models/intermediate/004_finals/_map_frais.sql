{{ config(materialized='ephemeral') }}
-- Mapping https://www.notion.so/gip-inclusion/champs-li-s-service-frais-1fb5f321b604804fba89e86358258c78
SELECT
    x.frais_v0,
    x.frais_v1
FROM (
    VALUES
    ('adhesion', 'payant'),
    ('gratuit', 'gratuit'),
    ('gratuit-sous-conditions', 'gratuit'),
    ('pass-numerique', 'gratuit'),
    ('payant', 'payant')
) AS x (frais_v0, frais_v1)
