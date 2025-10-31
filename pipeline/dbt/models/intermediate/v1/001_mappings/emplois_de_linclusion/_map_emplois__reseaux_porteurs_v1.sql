{{ config(materialized='ephemeral') }}
SELECT
    x.kind,
    x.reseaux_porteurs
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('ACI',        'aci'),
    ('AFPA',       'afpa'),
    ('ASE',        'ase'),
    ('CAARUD',     'caarud'),
    ('CADA',       'cada'),
    ('CAF',        'caf'),
    ('CAP_EMPLOI', 'cap-emploi-reseau-cheops'),
    ('CAVA',       'cava'),
    ('CCAS',       'ccas-cias'),
    ('CHRS',       'chrs'),
    ('CHU',        'chu'),
    ('CIDFF',      'cidff'),
    ('CPH',        'cph'),
    ('CSAPA',      'csapa'),
    ('DEPT',       'departements'),
    ('E2C',        'ecoles-de-la-deuxieme-chance'),
    ('EA',         'ea'),
    ('EATT',       'eatt'),
    ('EI',         'ei'),
    ('EITI',       'eiti'),
    ('EPIDE',      'epide'),
    ('ETTI',       'etti'),
    ('FT',         'france-travail'),
    ('GEIQ',       'geiq'),
    ('HUDA',       'huda'),
    ('ML',         'mission-locale'),
    ('MSA',        'mutualite-sociale-agricole'),
    ('OACAS',      NULL),
    ('OCASF',      NULL),
    ('OHPD',       NULL),
    ('OIL',        NULL),
    ('OTHER',      NULL),
    ('PENSION',    NULL),
    ('PJJ',        'pjj'),
    ('PLIE',       'plie'),
    ('PREVENTION', NULL),
    ('RS_FJT',     'residences-fjt'),
    ('SPIP',       'spip')
    -- noqa: enable=layout.spacing
) AS x (kind, reseaux_porteurs)
