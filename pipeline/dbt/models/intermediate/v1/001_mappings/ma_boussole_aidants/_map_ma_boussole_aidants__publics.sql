{{ config(materialized='ephemeral') }}
SELECT
    x.situation_id,
    UNNEST(x.publics) AS "public"
FROM (
    VALUES
    (1, ARRAY['jeunes']),
    (2, ARRAY['demandeurs-emploi', 'actifs']),
    (3, ARRAY['seniors'])
) AS x (situation_id, publics)
