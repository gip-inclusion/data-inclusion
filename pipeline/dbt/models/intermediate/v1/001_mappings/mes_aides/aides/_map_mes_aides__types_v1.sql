{{ config(materialized='ephemeral') }}
SELECT x.*
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('accompagnement',  'accompagnement'),
    ('aide financiere', 'aide-financiere'),
    ('allocation',      'aide-financiere'),
    ('prestation',      'aide-materielle')
    -- noqa: enable=layout.spacing
) AS x (type_aide, type)
