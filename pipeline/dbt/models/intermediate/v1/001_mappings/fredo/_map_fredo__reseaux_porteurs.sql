{{ config(materialized='ephemeral') }}
-- These are the most frequent categories at time of writing.
-- Given the large number of categories and for maintainability reasons,
-- we only handle the most frequent categories and leave it to fredo
-- to better categorize.
SELECT x.*
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('association',                              NULL),
    ('collectivité territoriale',                NULL),
    ('collectivité territoriale de la région',   'regions'),
    ('collectivité territoriale de la ville',    'communes'),
    ('collectivité territoriale du département', 'departements'),
    ('entreprise',                               NULL),
    ('etablissement privé',                      NULL),
    ('etablissement public',                     NULL),
    ('services de l''etat',                      NULL)
    -- noqa: enable=layout.spacing
) AS x (type_structure, reseau_porteur)
WHERE x.reseau_porteur IS NOT NULL
