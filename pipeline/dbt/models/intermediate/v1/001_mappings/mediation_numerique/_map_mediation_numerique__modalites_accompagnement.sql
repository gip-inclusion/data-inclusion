{{ config(materialized='ephemeral') }}

SELECT
    x.modalite_source,
    x.type AS "type"
FROM (
    VALUES
    ('Accompagnement individuel', 'accompagnement'),
    ('À distance', NULL),
    ('Dans un atelier collectif', 'atelier'),
    ('En autonomie', 'aide-materielle')
) AS x (modalite_source, type)
