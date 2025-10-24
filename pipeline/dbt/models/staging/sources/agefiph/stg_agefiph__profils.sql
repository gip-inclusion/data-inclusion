{{ config(materialized='ephemeral') }}

-- https://www.agefiph.fr/jsonapi/taxonomy_term/public_cible

SELECT x.*
FROM (
    VALUES
    ('df85cc61-d7dc-451f-8cd4-dff0af8dc753', 'Acteur de la formation'),
    ('1d0dd410-84b8-4d1e-a588-48c85b9216d1', 'Acteur de la santé'),
    ('d9425e1c-6075-46a8-a47d-7f52cd461575', 'Conseiller professionnel'),
    ('4c67f74c-f56e-49af-9809-805fcf32c23a', 'Employeur'),
    ('2ed6c723-b3c7-491d-b186-4c0b85d646ca', 'Partenaires sociaux'),
    ('0d0b63b6-4043-4b2d-a3f6-d7c85f335070', 'Personne en situation de handicap')
) AS x (id, name)
