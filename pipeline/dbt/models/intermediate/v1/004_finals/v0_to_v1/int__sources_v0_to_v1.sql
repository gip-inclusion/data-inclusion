{{ config(materialized='ephemeral') }}
SELECT x.*
FROM (
    VALUES
    ('mission-locale'),
    ('mes-aides')
) AS x (source)
