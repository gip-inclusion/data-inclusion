{{ config(materialized='ephemeral') }}
SELECT x.*
FROM (
    VALUES
    ('action-logement'),
    ('agefiph'),
    ('carif-oref'),
    ('dora'),
    ('emplois-de-linclusion'),
    ('france-travail'),
    ('ma-boussole-aidants'),
    ('monenfant'),
    ('soliguide')
) AS x (source)
