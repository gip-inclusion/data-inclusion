{{ config(materialized='ephemeral') }}
SELECT
    x.kind,
    x.reseaux_porteurs
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('ACI',        ARRAY['siae', 'siae-aci']),
    ('AFPA',       ARRAY['afpa']),
    ('ASE',        ARRAY['ase']),
    ('CAARUD',     ARRAY['caarud']),
    ('CADA',       ARRAY['cada']),
    ('CAF',        ARRAY['caf']),
    ('CAP_EMPLOI', ARRAY['cap-emploi-reseau-cheops']),
    ('CAVA',       ARRAY['cava']),
    ('CCAS',       ARRAY['ccas-cias']),
    ('CHRS',       ARRAY['chrs']),
    ('CHU',        ARRAY['chu']),
    ('CIDFF',      ARRAY['cidff']),
    ('CPH',        ARRAY['cph']),
    ('CSAPA',      ARRAY['csapa']),
    ('DEPT',       ARRAY['departements']),
    ('E2C',        ARRAY['ecoles-de-la-deuxieme-chance']),
    ('EA',         ARRAY['ea']),
    ('EATT',       ARRAY['eatt']),
    ('EI',         ARRAY['siae', 'siae-ei']),
    ('EITI',       ARRAY['siae', 'siae-eiti']),
    ('EPIDE',      ARRAY['epide']),
    ('ETTI',       ARRAY['siae', 'siae-etti']),
    ('FT',         ARRAY['france-travail']),
    ('GEIQ',       ARRAY['geiq']),
    ('HUDA',       ARRAY['huda']),
    ('ML',         ARRAY['mission-locale']),
    ('MSA',        ARRAY['mutualite-sociale-agricole']),
    ('OACAS',      NULL),
    ('OCASF',      NULL),
    ('OHPD',       NULL),
    ('OIL',        NULL),
    ('OTHER',      NULL),
    ('PENSION',    NULL),
    ('PJJ',        ARRAY['pjj']),
    ('PLIE',       ARRAY['plie']),
    ('PREVENTION', ARRAY['departements']),
    ('PREVENTION', NULL),
    ('RS_FJT',     ARRAY['residences-fjt']),
    ('SPIP',       ARRAY['spip'])
    -- noqa: enable=layout.spacing
) AS x (kind, reseaux_porteurs)
