{{ config(materialized='ephemeral') }}
SELECT
    x.public_soliguide,
    x.public_datainclusion
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('addiction',    'personnes-en-situation-durgence'),
    ('asylum',       'personnes-exilees'),
    ('couple',       'familles'),
    ('family',       'familles'),
    ('handicap',     'personnes-en-situation-de-handicap'),
    ('hiv',          NULL),
    ('isolated',     'personnes-en-situation-durgence'),
    ('isolated',     'seniors'),
    ('lgbt',         NULL),
    ('men',          NULL),
    ('mentalHealth', NULL),
    ('pregnant',     'femmes'),
    ('pregnant',     'familles'),
    ('prison',       'personnes-en-situation-juridique-specifique'),
    ('prostitution', NULL),
    ('refugee',      'personnes-exilees'),
    ('regular',      NULL),
    ('student',      'etudiants'),
    ('undocumented', 'personnes-exilees'),
    ('violence',     'personnes-en-situation-durgence'),
    ('women',        'femmes'),
    ('tous-publics', 'tous-publics')
    -- noqa: enable=layout.spacing
) AS x (public_soliguide, public_datainclusion)
