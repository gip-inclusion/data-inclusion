{{ config(materialized='ephemeral') }}

SELECT
    x.public_source,
    x.public AS "public"
FROM (
    VALUES
    ('Étudiants', 'etudiants'),
    ('Familles et/ou enfants', 'familles'),
    ('Femmes', 'femmes'),
    ('Jeunes', 'jeunes'),
    ('Seniors', 'seniors')
) AS x (public_source, public)
