{{ config(materialized='ephemeral') }}
SELECT
    x.source,
    x.reseau_porteur
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('Alisol (AD2S)',                 NULL),
    ('Conseil Départemental du 59',   'departements'),
    ('Croix-Rouge française',         'croix-rouge'),
    ('Crous - Pays de la Loire',      NULL),
    ('Société Saint Vincent de Paul', NULL),
    ('restos',                        'restos-du-coeur')
    -- noqa: enable=layout.spacing
) AS x (source, reseau_porteur)
