{{ config(materialized='ephemeral') }}

SELECT
    x.frais_source,
    x.frais AS "frais"
FROM (
    VALUES
    ('Gratuit', 'gratuit'),
    ('Gratuit sous condition', 'gratuit'),
    ('Payant', 'payant')
) AS x (frais_source, frais)
