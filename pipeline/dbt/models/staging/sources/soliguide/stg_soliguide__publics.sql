-- https://solinum.notion.site/d1d4fedbb49f457693e7bde5589046fd?v=cd5e8879302146d79d4b9aef4ccb8a95&p=e189aa3626a44aea95a8a5e9c5b8c09f&pm=s

-- Soliguide describes publics using 4 dimensions : administrative status, familial status, gender status, other status.
-- Each dimension partitions publics into several values.
-- For each dimension, when all possible values are provided, it means that the dimension is not relevant for the service.

SELECT *
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('administrative', 'regular'),
    ('administrative', 'asylum'),
    ('administrative', 'refugee'),
    ('administrative', 'undocumented'),
    ('familialle',     'isolated'),
    ('familialle',     'family'),
    ('familialle',     'couple'),
    ('familialle',     'pregnant'),
    ('gender',         'men'),
    ('gender',         'women'),
    ('other',          'violence'),
    ('other',          'addiction'),
    ('other',          'handicap'),
    ('other',          'lgbt'),
    ('other',          'hiv'),
    ('other',          'prostitution'),
    ('other',          'prison'),
    ('other',          'student'),
    ('other',          'mentalHealth')
    -- noqa: enable=layout.spacing
) AS x (dimension, value)
