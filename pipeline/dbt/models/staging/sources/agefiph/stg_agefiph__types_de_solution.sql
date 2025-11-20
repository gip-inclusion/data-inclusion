{{ config(materialized='ephemeral') }}

-- https://www.agefiph.fr/jsonapi/taxonomy_term/solution_type

SELECT x.*
FROM (
    VALUES
    ('aec45130-893a-45ba-84e0-41ff7bd99815', 'Aide financière'),
    ('f7e83615-cb00-4ddd-91ee-9586d86ccf23', 'Service'),
    ('dc2ce6a8-3fe2-439c-ae32-f67b720516a0', 'Outil'),
    ('95aa6e8c-aeae-4459-bbde-6ec271a8217c', 'Réseau d''échanges')
) AS x (id, name)
