{{ config(materialized='ephemeral') }}
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
