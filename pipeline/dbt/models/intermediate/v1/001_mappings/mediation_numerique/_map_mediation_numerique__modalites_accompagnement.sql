{{ config(materialized='ephemeral') }}

SELECT
    x.modalite_source,
    x.mode_accueil AS "mode_accueil"
FROM (
    VALUES
    ('Accompagnement individuel', 'en-presentiel'),
    ('À distance', 'a-distance'),
    ('Dans un atelier collectif', 'en-presentiel'),
    ('En autonomie', 'en-presentiel')
) AS x (modalite_source, mode_accueil)
