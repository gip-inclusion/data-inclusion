{{ config(materialized='ephemeral') }}
SELECT
    x.profil_v0,
    x.public_v1
FROM (
    VALUES
    ('adultes', NULL),
    ('alternants', 'etudiants'),
    ('alternants', 'actifs'),
    ('beneficiaires-rsa', 'beneficiaires-des-minimas-sociaux'),
    ('deficience-visuelle', 'personnes-en-situation-de-handicap'),
    ('handicaps-mentaux', 'personnes-en-situation-de-handicap'),
    ('handicaps-psychiques', 'personnes-en-situation-de-handicap'),
    ('personnes-en-situation-de-handicap', 'personnes-en-situation-de-handicap'),
    ('personnes-handicapees', 'personnes-en-situation-de-handicap'),
    ('surdite', 'personnes-en-situation-de-handicap'),
    ('demandeurs-demploi', 'demandeurs-emploi'),
    ('etudiants', 'etudiants'),
    ('familles-enfants', 'familles'),
    ('femmes', 'femmes'),
    ('jeunes', 'jeunes'),
    ('jeunes-16-26', 'jeunes'),
    ('locataires', 'actifs'),
    ('proprietaires', 'actifs'),
    ('personnes-de-nationalite-etrangere', 'personnes-exilees'),
    ('public-langues-etrangeres', 'personnes-exilees'),
    ('personnes-en-situation-durgence', 'personnes-en-situation-durgence'),
    ('personnes-en-situation-illettrisme', 'personnes-en-situation-durgence'),
    ('sans-domicile-fixe', 'personnes-en-situation-durgence'),
    ('sortants-de-detention', 'personnes-en-situation-durgence'),
    ('victimes', 'personnes-en-situation-durgence'),
    ('retraites', 'seniors'),
    ('seniors-65', 'seniors'),
    ('salaries', 'actifs'),
    ('tous-publics', 'tous-publics')
) AS x (profil_v0, public_v1)
