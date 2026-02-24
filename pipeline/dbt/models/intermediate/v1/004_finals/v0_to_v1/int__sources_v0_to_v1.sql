{{ config(materialized='ephemeral') }}
SELECT x.*
FROM (
    VALUES
    ('mission-locale')
) AS x (source)
