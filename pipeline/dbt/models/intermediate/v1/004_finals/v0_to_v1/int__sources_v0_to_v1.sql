{{ config(materialized='ephemeral') }}
SELECT x.*
FROM (
    VALUES
    ('action-logement'),
    ('agefiph'),
    ('carif-oref'),
    ('cd35'),
    ('dora'),
    ('emplois-de-linclusion'),
    ('france-travail'),
    ('fredo'),
    ('ma-boussole-aidants'),
    ('monenfant'),
    ('soliguide'),
    ('reseau-alpha')
) AS x (source)
