{{ config(materialized='ephemeral') }}

SELECT
    x.modalite,
    x.type
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('Accompagnement individuel', 'accompagnement'),
    ('À distance',                NULL),
    ('Dans un atelier collectif', 'atelier'),
    ('En autonomie',              'aide-materielle')
    -- noqa: enable=layout.spacing
) AS x (modalite, type)
WHERE x.type IS NOT NULL
