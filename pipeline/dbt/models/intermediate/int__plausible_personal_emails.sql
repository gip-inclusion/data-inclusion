WITH structures AS (
    SELECT * FROM {{ ref('int__union_structures') }}
),

prenoms AS (
    SELECT * FROM {{ ref('int_extra__insee_prenoms_filtered') }}
),

structures_with_suspect_emails AS (
    SELECT
        _di_surrogate_id,
        courriel,
        nom,
        LOWER(SPLIT_PART(courriel, '@', 1)) AS courriel_local_part,
        LOWER(SPLIT_PART(courriel, '@', 2)) AS courriel_domain
    FROM structures
    WHERE
        courriel IS NOT NULL
        -- the usual suspects
        AND SPLIT_PART(courriel, '@', 2) IN (
            'wanadoo.fr',
            'orange.fr',
            'gmail.com',
            'free.fr',
            'laposte.net',
            'yahoo.fr',
            'hotmail.fr',
            'nordnet.fr',
            'sfr.fr',
            'outlook.fr',
            'laposte.fr'
        )
        -- ignore common patterns for non personal emails
        AND NOT SPLIT_PART(courriel, '@', 1) ~ 'mairie|commune|adil'
),

plausible_personal_emails AS (
    SELECT
        structures_with_suspect_emails._di_surrogate_id,
        structures_with_suspect_emails.courriel
    FROM
        structures_with_suspect_emails,
        prenoms
    WHERE
        -- email starts with a known firstname
        structures_with_suspect_emails.courriel_local_part ~ (
            '^' || prenoms.prenom || '\.'
        )
        -- email is not to similar to the structure name
        AND WORD_SIMILARITY(
            structures_with_suspect_emails.courriel_local_part,
            structures_with_suspect_emails.nom
        ) < 0.17
)

SELECT * FROM plausible_personal_emails
