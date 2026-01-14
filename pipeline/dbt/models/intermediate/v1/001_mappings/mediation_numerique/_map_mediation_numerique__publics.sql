{{ config(materialized='ephemeral') }}

SELECT
    x.public_source,
    x.public
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('Étudiants',              'etudiants'),
    ('Familles et/ou enfants', 'familles'),
    ('Femmes',                 'femmes'),
    ('Jeunes',                 'jeunes'),
    ('Seniors',                'seniors')
    -- noqa: enable=layout.spacing
) AS x (public_source, public)
WHERE x.public IS NOT NULL
